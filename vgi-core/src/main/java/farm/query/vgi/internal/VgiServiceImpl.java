// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.SettingSpec;
import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.AggregateBindRequest;
import farm.query.vgi.protocol.AggregateBindResponse;
import farm.query.vgi.protocol.AggregateCombineRequest;
import farm.query.vgi.protocol.AggregateDestructorRequest;
import farm.query.vgi.protocol.AggregateFinalizeRequest;
import farm.query.vgi.protocol.AggregateFinalizeResponse;
import farm.query.vgi.protocol.AggregateUpdateRequest;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CardinalityRequest;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.types.pojo.Schema;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class VgiServiceImpl implements VgiService {

    private final Worker worker;
    private final Map<String, List<ScalarFunction>> scalars = new HashMap<>();
    private final Map<String, List<TableFunction>> tables = new HashMap<>();
    private final Map<String, List<TableInOutFunction>> tableInOuts = new HashMap<>();
    private final Map<String, List<AggregateFunction<?>>> aggregates = new HashMap<>();
    private final AggregateRunner aggregateRunner;
    /**
     * Bind cache keyed by an opaque token returned to DuckDB as
     * {@link BindResponse#opaque_data} and echoed back via
     * {@link InitRequest#bind_opaque_data}. Concurrent in-flight calls to the
     * same function (POSITIONAL JOIN, self-join) get distinct tokens.
     */
    private final java.util.Map<String, BoundEntry> pendingBinds = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * Per-execution table-in-out exchange states, keyed by hex execution_id.
     * Survives the INPUT→FINALIZE phase boundary so that buffered state
     * accumulated during exchange ticks can be drained at finalize time.
     */
    private final java.util.Map<String, TioExecutionState> tioExecutions = new java.util.concurrent.ConcurrentHashMap<>();

    /** Snapshot of an in-flight TIO execution, kept alive for FINALIZE-phase init. */
    private record TioExecutionState(TableInOutFunction fn, TableInOutExchangeState state,
                                       TableInOutInitParams params) {}
    private final SecureRandom rng = new SecureRandom();

    private byte[] currentAttachId;

    /** Cached state from a {@code bind} call, picked up by the matching {@code init}. */
    private sealed interface BoundEntry {
        Arguments args();
        Schema inputSchema();
        Schema outputSchema();
        Map<String, Object> settings();
        byte[] argumentsIpc();
        byte[] settingsIpc();
        byte[] outputSchemaIpc();
    }

    private record BoundScalar(ScalarFunction fn, Arguments args, Schema inputSchema,
                                Schema outputSchema, Map<String, Object> settings,
                                byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc)
            implements BoundEntry {}

    private record BoundTable(TableFunction fn, Arguments args, Schema inputSchema,
                               Schema outputSchema, Map<String, Object> settings,
                               byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc)
            implements BoundEntry {}

    private record BoundTableInOut(TableInOutFunction fn, Arguments args, Schema inputSchema,
                                    Schema outputSchema, Map<String, Object> settings,
                                    byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc)
            implements BoundEntry {}

    public VgiServiceImpl(Worker worker, List<ScalarFunction> scalars, List<TableFunction> tables,
                           List<TableInOutFunction> tableInOuts, List<AggregateFunction<?>> aggregates) {
        this.worker = worker;
        for (ScalarFunction f : scalars) this.scalars.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        for (TableFunction f : tables) this.tables.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        for (TableInOutFunction f : tableInOuts) this.tableInOuts.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        for (AggregateFunction<?> f : aggregates) this.aggregates.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        // Aggregate runner expects flat name → fn (no overloads in scope yet).
        Map<String, AggregateFunction<?>> aggFlat = new HashMap<>();
        for (var e : this.aggregates.entrySet()) aggFlat.put(e.getKey(), e.getValue().get(0));
        this.aggregateRunner = new AggregateRunner(aggFlat);
        ServiceLocator.setCurrent(new ServiceLocator(
                this.scalars, this.tables, this.tableInOuts, this.aggregates));
    }

    /** Pick the variant whose argument-spec count matches the arg count seen at bind. */
    private static <T> T pickVariant(List<T> variants, int argCount) {
        return pickVariant(variants, argCount, null, null);
    }

    /**
     * Type-aware variant pick. When two variants share the same arity,
     * compares each declared {@link farm.query.vgi.function.ArgSpec#arrowType}
     * against the actual argument's type — const args use the parsed value's
     * Java class, column args use {@code inputSchema}'s field type at the
     * column's position.
     */
    private static <T> T pickVariant(List<T> variants, int argCount,
                                       Arguments args, Schema inputSchema) {
        if (variants == null || variants.isEmpty()) return null;
        if (variants.size() == 1) return variants.get(0);

        // First filter by arity — accept exact matches plus varargs catch-alls.
        List<T> matching = new ArrayList<>();
        for (T v : variants) {
            int n = countArgs(v);
            if (n == argCount) { matching.add(v); continue; }
            if (hasVarargs(v) && argCount >= n) matching.add(v);
        }
        if (matching.isEmpty()) matching = variants;
        if (matching.size() == 1) return matching.get(0);

        // Tiebreak by argument type. Score each variant by the number of
        // positions whose declared type aligns with the actual argument type.
        T best = matching.get(0);
        int bestScore = scoreTypeMatch(best, args, inputSchema);
        for (int i = 1; i < matching.size(); i++) {
            T v = matching.get(i);
            int score = scoreTypeMatch(v, args, inputSchema);
            if (score > bestScore) { bestScore = score; best = v; }
        }
        return best;
    }

    private static int countArgs(Object fn) {
        if (fn instanceof ScalarFunction f) return f.argumentSpecs().size();
        if (fn instanceof TableFunction f) return f.argumentSpecs().size();
        if (fn instanceof TableInOutFunction f) return f.argumentSpecs().size();
        return -1;
    }

    private static boolean hasVarargs(Object fn) {
        List<farm.query.vgi.function.ArgSpec> specs = argSpecs(fn);
        if (specs == null || specs.isEmpty()) return false;
        for (farm.query.vgi.function.ArgSpec s : specs) if (s.varargs()) return true;
        return false;
    }

    private static List<farm.query.vgi.function.ArgSpec> argSpecs(Object fn) {
        if (fn instanceof ScalarFunction f) return f.argumentSpecs();
        if (fn instanceof TableFunction f) return f.argumentSpecs();
        if (fn instanceof TableInOutFunction f) return f.argumentSpecs();
        return null;
    }

    /**
     * Per-position type-match score. Each position whose declared type
     * matches the actual arg's type adds 1; an {@code anyType} ArgSpec is
     * neutral (0); a mismatch subtracts 1 so explicit-type variants
     * outrank "any" variants when they fit, and lose when they don't.
     */
    private static int scoreTypeMatch(Object fn, Arguments args, Schema inputSchema) {
        List<farm.query.vgi.function.ArgSpec> specs = argSpecs(fn);
        if (specs == null) return 0;
        int constN = args == null ? 0 : args.positional().size();
        int colN = inputSchema == null ? 0 : inputSchema.getFields().size();
        int total = constN + colN;
        int score = 0;
        for (int i = 0; i < total; i++) {
            farm.query.vgi.function.ArgSpec spec = specAt(specs, i);
            if (spec == null || spec.anyType()) continue;
            org.apache.arrow.vector.types.pojo.ArrowType expected = spec.arrowType();
            org.apache.arrow.vector.types.pojo.ArrowType actual;
            if (i < constN) actual = inferConstType(args.positional().get(i));
            else actual = inputSchema.getFields().get(i - constN).getType();
            if (actual == null || expected == null) continue;
            if (typesAlign(expected, actual)) score += 1;
            else score -= 1;
        }
        return score;
    }

    private static farm.query.vgi.function.ArgSpec specAt(
            List<farm.query.vgi.function.ArgSpec> specs, int position) {
        for (farm.query.vgi.function.ArgSpec s : specs) {
            if (s.position() == position) return s;
        }
        // Varargs spread: the last varargs spec absorbs all positions >= its position.
        farm.query.vgi.function.ArgSpec va = null;
        for (farm.query.vgi.function.ArgSpec s : specs) {
            if (s.varargs() && s.position() >= 0 && s.position() <= position) va = s;
        }
        return va;
    }

    private static org.apache.arrow.vector.types.pojo.ArrowType inferConstType(Object v) {
        if (v == null) return null;
        if (v instanceof Boolean) return new org.apache.arrow.vector.types.pojo.ArrowType.Bool();
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true);
        }
        if (v instanceof Double || v instanceof Float) {
            return new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                    org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        }
        if (v instanceof CharSequence) return new org.apache.arrow.vector.types.pojo.ArrowType.Utf8();
        if (v instanceof byte[]) return new org.apache.arrow.vector.types.pojo.ArrowType.Binary();
        return null;
    }

    private static boolean typesAlign(org.apache.arrow.vector.types.pojo.ArrowType expected,
                                        org.apache.arrow.vector.types.pojo.ArrowType actual) {
        if (expected.getClass() != actual.getClass()) return false;
        // Strict match for Int — distinguish int32/int64/uint32/uint64 so
        // type-dispatched fixtures like type_info pick the right variant.
        if (expected instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int e
                && actual instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int a) {
            return e.getBitWidth() == a.getBitWidth() && e.getIsSigned() == a.getIsSigned();
        }
        // Strict match for FloatingPoint — float vs double.
        if (expected instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint e
                && actual instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint a) {
            return e.getPrecision() == a.getPrecision();
        }
        return expected.equals(actual);
    }

    // -----------------------------------------------------------------------
    // Function execution
    // -----------------------------------------------------------------------

    @Override
    public BindResponse bind(BindRequest request, CallContext ctx) {
        String name = request.function_name();
        Schema inputSchema = SchemaUtil.deserializeSchema(request.input_schema());
        Arguments args = ArgumentsParser.parse(request.arguments());
        Map<String, Object> settings = SettingsParser.parse(request.settings());

        // Per-bind token used as both BindResponse.opaque_data and the
        // pendingBinds key. DuckDB echoes it back via InitRequest.bind_opaque_data.
        byte[] token = new byte[16];
        rng.nextBytes(token);
        String key = bytesKey(token);

        // argCount = const args (in `arguments` field) + column args (in input_schema fields).
        int constArgCount = args.positional().size();
        int columnArgCount = inputSchema == null ? 0 : inputSchema.getFields().size();
        int argCount = constArgCount + columnArgCount;
        if (scalars.containsKey(name)) {
            ScalarFunction fn = pickVariant(scalars.get(name), argCount, args, inputSchema);
            BindResponse upstream = fn.onBind(new ScalarBindParams(name, args, inputSchema, settings));
            Schema outputSchema = upstream.output_schema() == null
                    ? null
                    : SchemaUtil.deserializeSchema(upstream.output_schema());
            pendingBinds.put(key, new BoundScalar(fn, args, inputSchema, outputSchema, settings,
                    request.arguments(), request.settings(), upstream.output_schema()));
            return new BindResponse(upstream.output_schema(), token,
                    upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
        }
        if (tables.containsKey(name)) {
            TableFunction fn = pickVariant(tables.get(name), argCount, args, inputSchema);
            BindResponse upstream = fn.onBind(new TableBindParams(name, args, inputSchema, settings));
            Schema outputSchema = upstream.output_schema() == null
                    ? null
                    : SchemaUtil.deserializeSchema(upstream.output_schema());
            pendingBinds.put(key, new BoundTable(fn, args, inputSchema, outputSchema, settings,
                    request.arguments(), request.settings(), upstream.output_schema()));
            return new BindResponse(upstream.output_schema(), token,
                    upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
        }
        if (tableInOuts.containsKey(name)) {
            TableInOutFunction fn = pickVariant(tableInOuts.get(name), argCount, args, inputSchema);
            BindResponse upstream = fn.onBind(new TableInOutBindParams(name, args, inputSchema, settings));
            Schema outputSchema = upstream.output_schema() == null
                    ? null
                    : SchemaUtil.deserializeSchema(upstream.output_schema());
            pendingBinds.put(key, new BoundTableInOut(fn, args, inputSchema, outputSchema, settings,
                    request.arguments(), request.settings(), upstream.output_schema()));
            return new BindResponse(upstream.output_schema(), token,
                    upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
        }
        throw new IllegalArgumentException("Unknown function: " + name);
    }

    @Override
    public RpcStream<? extends StreamState> init(InitRequest request, CallContext ctx) {
        BoundEntry bound = null;
        // Newer C++ extensions (post-commit 09be719) skip the bind RPC at the
        // init phase to save a round-trip; bind_call carries the inlined
        // BindRequest that we run inline. Older clients pre-call bind() and
        // we look up the cached entry by opaque_data.
        if (request.bind_opaque_data() != null) {
            String key = bytesKey(request.bind_opaque_data());
            bound = pendingBinds.get(key);
        }
        if (bound == null && request.bind_call() != null && request.bind_call().length > 0) {
            BindRequest embedded = RecordCodec.deserializeFromBytes(
                    request.bind_call(), BindRequest.class);
            // bind() returns a fresh opaque_data token; look the new entry up
            // by *that* token so concurrent inits to the same function don't
            // clobber each other (LRU iteration would pick the stale bind).
            BindResponse br = bind(embedded, ctx);
            String key = bytesKey(br.opaque_data());
            bound = pendingBinds.get(key);
            // The synthetic bind() above stays in the cache — purge so it
            // doesn't shadow future inline-bind calls.
            pendingBinds.remove(key);
        }
        if (bound == null) {
            throw new IllegalStateException("init: no bind context — bind_opaque_data=" +
                    (request.bind_opaque_data() == null ? "null" : "present") +
                    " bind_call_len=" + (request.bind_call() == null ? -1 : request.bind_call().length));
        }
        Schema realOutputSchema = SchemaUtil.deserializeSchema(request.output_schema());
        byte[] execId = request.execution_id() != null ? request.execution_id() : newExecutionId();
        GlobalInitResponse header = GlobalInitResponse.of(execId);

        if (bound instanceof BoundScalar bs) {
            Schema inputSchema = bs.inputSchema() != null ? bs.inputSchema() : new Schema(List.of());
            int variantIdx = ServiceLocator.current().scalarIndexOf(bs.fn().name(), bs.fn());
            ScalarStreamState state = new ScalarStreamState(
                    bs.fn().name(), bs.fn().argumentSpecs().size(), variantIdx,
                    request.output_schema(), bs.argumentsIpc(), bs.settingsIpc());
            return RpcStream.exchange(inputSchema, realOutputSchema, state, header);
        }
        if (bound instanceof BoundTable bt) {
            // Project the full output schema down to the columns DuckDB
            // requested (projection_ids), but only when the function opts
            // into projection pushdown. Otherwise the framework would have
            // to auto-project the emitted batches; not implemented yet, so
            // non-pushdown functions keep the full schema.
            List<Integer> projIds = request.projection_ids() == null
                    ? List.of() : request.projection_ids();
            Schema fnOutputSchema = realOutputSchema;
            if (bt.fn().metadata().projectionPushdown() && !projIds.isEmpty()) {
                fnOutputSchema = projectSchema(realOutputSchema, projIds);
            }
            TableInitParams params = new TableInitParams(
                    bt.fn().name(), bt.args(), fnOutputSchema, bt.settings(), Allocators.root(),
                    request.pushdown_filters(),
                    projIds,
                    request.join_keys() == null ? List.of() : request.join_keys(),
                    request.tablesample_percentage(),
                    request.tablesample_seed(),
                    request.order_by_column_name(),
                    request.order_by_direction(),
                    request.order_by_null_order(),
                    request.order_by_limit());
            TableProducerState state = bt.fn().createProducer(params);
            return RpcStream.producer(fnOutputSchema, state, header);
        }
        if (bound instanceof BoundTableInOut bio) {
            String phase = request.phase();
            String execKey = bytesKey(execId);
            if ("FINALIZE".equalsIgnoreCase(phase)) {
                // FINALIZE: look up the exchange state captured during INPUT
                // phase and emit any buffered batches via a producer stream.
                TioExecutionState saved = tioExecutions.get(execKey);
                java.util.List<org.apache.arrow.vector.VectorSchemaRoot> batches =
                        saved != null
                                ? bio.fn().finalizeBatches(saved.state(), saved.params())
                                : bio.fn().finalizeBatches(null,
                                        new TableInOutInitParams(bio.fn().name(), bio.args(),
                                                bio.inputSchema(), realOutputSchema,
                                                bio.settings(), Allocators.root()));
                tioExecutions.remove(execKey);
                farm.query.vgirpc.ProducerState producer = new FinalizeProducerState(batches);
                return RpcStream.producer(realOutputSchema, producer, header);
            }
            // INPUT phase
            Schema inputSchema = bio.inputSchema() != null ? bio.inputSchema() : new Schema(List.of());
            TableInOutInitParams params = new TableInOutInitParams(
                    bio.fn().name(), bio.args(), inputSchema, realOutputSchema,
                    bio.settings(), Allocators.root());
            TableInOutExchangeState state = bio.fn().createExchange(params);
            // Stash for the matching FINALIZE init.
            if (bio.fn().hasFinalize()) {
                tioExecutions.put(execKey, new TioExecutionState(bio.fn(), state, params));
            }
            return RpcStream.exchange(inputSchema, realOutputSchema, state, header);
        }
        throw new IllegalStateException("Unexpected bound type: " + bound);
    }

    private static Schema projectSchema(Schema full, List<Integer> projectionIds) {
        List<org.apache.arrow.vector.types.pojo.Field> picked = new ArrayList<>(projectionIds.size());
        for (int idx : projectionIds) {
            if (idx >= 0 && idx < full.getFields().size()) picked.add(full.getFields().get(idx));
        }
        return picked.isEmpty() ? full : new Schema(picked);
    }

    private static String bytesKey(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    private byte[] newExecutionId() {
        UUID u = UUID.randomUUID();
        byte[] out = new byte[16];
        long msb = u.getMostSignificantBits();
        long lsb = u.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) out[i] = (byte) (msb >>> (8 * (7 - i)));
        for (int i = 0; i < 8; i++) out[i + 8] = (byte) (lsb >>> (8 * (7 - i)));
        return out;
    }

    // -----------------------------------------------------------------------
    // Cardinality (table functions only; scalars never get this call)
    // -----------------------------------------------------------------------

    @Override
    public farm.query.vgi.protocol.CardinalityResponse table_function_cardinality(CardinalityRequest request) {
        long result = computeCardinality(request);
        return new farm.query.vgi.protocol.CardinalityResponse(
                result < 0 ? null : result,
                result < 0 ? null : result);
    }

    private long computeCardinality(CardinalityRequest request) {
        if (request.bind_opaque_data() != null) {
            BoundEntry e = pendingBinds.get(bytesKey(request.bind_opaque_data()));
            if (e instanceof BoundTable bt) {
                return bt.fn().cardinality(new TableBindParams(
                        bt.fn().name(), bt.args(), bt.inputSchema(), bt.settings()));
            }
        }
        if (request.bind_call() != null && request.bind_call().length > 0) {
            BindRequest embedded = RecordCodec.deserializeFromBytes(request.bind_call(), BindRequest.class);
            if (tables.containsKey(embedded.function_name())) {
                Schema inputSchema = SchemaUtil.deserializeSchema(embedded.input_schema());
                Arguments args = ArgumentsParser.parse(embedded.arguments());
                Map<String, Object> settings = SettingsParser.parse(embedded.settings());
                int constN = args.positional().size();
                int colN = inputSchema == null ? 0 : inputSchema.getFields().size();
                TableFunction fn = pickVariant(tables.get(embedded.function_name()),
                        constN + colN, args, inputSchema);
                return fn.cardinality(new TableBindParams(embedded.function_name(),
                        args, inputSchema, settings));
            }
        }
        return -1L;
    }

    // -----------------------------------------------------------------------
    // Aggregate function lifecycle
    // -----------------------------------------------------------------------

    @Override
    public AggregateBindResponse aggregate_bind(AggregateBindRequest request) {
        return aggregateRunner.bind(request.function_name(), request.input_schema(), request.arguments());
    }

    @Override
    public farm.query.vgi.protocol.AggregateUpdateResponse aggregate_update(AggregateUpdateRequest request) {
        aggregateRunner.update(request.function_name(), request.execution_id(), request.input_batch());
        return new farm.query.vgi.protocol.AggregateUpdateResponse();
    }

    @Override
    public farm.query.vgi.protocol.AggregateCombineResponse aggregate_combine(AggregateCombineRequest request) {
        aggregateRunner.combine(request.function_name(), request.execution_id(), request.merge_batch());
        return new farm.query.vgi.protocol.AggregateCombineResponse();
    }

    @Override
    public AggregateFinalizeResponse aggregate_finalize(AggregateFinalizeRequest request) {
        return aggregateRunner.finalizeRequest(
                request.function_name(), request.execution_id(),
                request.group_ids_batch(), request.output_schema());
    }

    @Override
    public farm.query.vgi.protocol.AggregateDestructorResponse aggregate_destructor(AggregateDestructorRequest request) {
        aggregateRunner.destructor(
                request.function_name(), request.execution_id(), request.group_ids_batch());
        return new farm.query.vgi.protocol.AggregateDestructorResponse();
    }

    // -----------------------------------------------------------------------
    // Read-only catalog
    // -----------------------------------------------------------------------

    @Override
    public ItemsResponse catalog_catalogs() {
        return ItemsResponse.empty();
    }

    @Override
    public farm.query.vgi.protocol.CatalogVersionResponse catalog_version(byte[] attach_id, byte[] transaction_id) {
        return new farm.query.vgi.protocol.CatalogVersionResponse(1L);
    }

    @Override
    public CatalogAttachResult catalog_attach(CatalogAttachRequest request, CallContext ctx) {
        byte[] attachId = new byte[16];
        rng.nextBytes(attachId);
        currentAttachId = attachId;
        List<byte[]> settings = new ArrayList<>();
        for (SettingSpec spec : worker.settingSpecs()) {
            settings.add(SettingSpecSerializer.serialize(spec));
        }
        return new CatalogAttachResult(
                attachId,
                false, false, false,
                1L,
                false,
                worker.defaultSchema(),
                settings,
                List.of(),
                worker.catalogComment(),
                worker.catalogTags(),
                false,
                null,
                null);
    }

    @Override
    public void catalog_detach(byte[] attach_id) {
        if (Arrays.equals(currentAttachId, attach_id)) currentAttachId = null;
    }

    @Override
    public ItemsResponse catalog_schemas(byte[] attach_id, byte[] transaction_id) {
        List<byte[]> items = new ArrayList<>();
        for (SchemaDesc s : workerSchemas()) {
            items.add(RecordCodec.serializeToBytes(
                    new SchemaInfo(s.comment, Map.of(), attach_id, s.name, schemaCounts(s))));
        }
        return new ItemsResponse(items);
    }

    @Override
    public ItemsResponse catalog_schema_get(byte[] attach_id, String name, byte[] transaction_id) {
        for (SchemaDesc s : workerSchemas()) {
            if (s.name.equals(name)) {
                return new ItemsResponse(List.of(RecordCodec.serializeToBytes(
                        new SchemaInfo(s.comment, Map.of(), attach_id, name, schemaCounts(s)))));
            }
        }
        return ItemsResponse.empty();
    }

    /** Schema descriptors registered with the worker (default + auxiliary). */
    private record SchemaDesc(String name, String comment) {}

    private List<SchemaDesc> workerSchemas() {
        return List.of(
                new SchemaDesc(worker.defaultSchema(), "Example functions for testing VGI"),
                new SchemaDesc("data", "Example tables backed by functions"));
    }

    /**
     * Per-schema object counts. {@code 0} for a kind tells the C++ extension
     * to skip the corresponding {@code catalog_schema_contents_*} RPC entirely.
     * We have no tables/views/macros/indexes yet — declaring 0 lets the
     * extension short-circuit those scans.
     */
    private Map<String, Long> schemaCounts(SchemaDesc s) {
        Map<String, Long> m = new java.util.LinkedHashMap<>();
        if (s.name.equals(worker.defaultSchema())) {
            long fnCount = 0;
            for (var v : scalars.values()) fnCount += v.size();
            for (var v : tables.values()) fnCount += v.size();
            for (var v : tableInOuts.values()) fnCount += v.size();
            for (var v : aggregates.values()) fnCount += v.size();
            m.put("functions", fnCount);
        } else {
            m.put("functions", 0L);
        }
        long tableCount = worker.catalogTables().stream()
                .filter(t -> t.schema().equals(s.name))
                .count();
        m.put("tables", tableCount);
        long viewCount = worker.views().stream()
                .filter(v -> v.schema().equals(s.name))
                .count();
        m.put("views", viewCount);
        long macroCount = worker.macros().stream()
                .filter(macro -> macro.schema().equals(s.name))
                .count();
        m.put("macros", macroCount);
        m.put("indexes", 0L);
        return m;
    }

    @Override
    public ItemsResponse catalog_schema_contents_tables(
            byte[] attach_id, String name, byte[] transaction_id) {
        List<byte[]> items = new ArrayList<>();
        for (Worker.CatalogTable t : worker.catalogTables()) {
            if (!t.schema().equals(name)) continue;
            items.add(TableInfoSerializer.serialize(toTableInfo(t)));
        }
        return new ItemsResponse(items);
    }

    @Override
    public farm.query.vgi.protocol.TableScanFunctionGetResponse catalog_table_scan_function_get(
            byte[] attach_id, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_id) {
        for (Worker.CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name) && t.scanFunctionName() != null) {
                byte[] argsBytes = ScanFunctionResultEncoder.encodeArguments(
                        t.scanFunctionPositional() == null ? List.of() : t.scanFunctionPositional(),
                        t.scanFunctionNamed() == null ? Map.of() : t.scanFunctionNamed());
                return new farm.query.vgi.protocol.TableScanFunctionGetResponse(
                        t.scanFunctionName(), argsBytes, List.of());
            }
        }
        throw new IllegalArgumentException("scan_function_get: unknown table " + schema_name + "." + name);
    }

    @Override
    public ItemsResponse catalog_table_get(
            byte[] attach_id, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_id) {
        for (Worker.CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                return new ItemsResponse(List.of(TableInfoSerializer.serialize(toTableInfo(t))));
            }
        }
        return ItemsResponse.empty();
    }

    private farm.query.vgi.protocol.TableInfo toTableInfo(Worker.CatalogTable t) {
        byte[] scanFn = null;
        if (t.scanFunctionName() != null && t.inlineScanFunction()) {
            scanFn = ScanFunctionResultEncoder.encode(
                    t.scanFunctionName(),
                    t.scanFunctionPositional() == null ? List.of() : t.scanFunctionPositional(),
                    t.scanFunctionNamed() == null ? Map.of() : t.scanFunctionNamed(),
                    List.of());
        }
        return new farm.query.vgi.protocol.TableInfo(
                t.comment() == null || t.comment().isEmpty() ? null : t.comment(),
                t.tags() == null ? Map.of() : t.tags(),
                t.name(),
                t.schema(),
                t.columns() == null ? new byte[0] : t.columns(),
                List.of(),     // not_null_constraints
                List.of(),     // unique_constraints
                List.of(),     // check_constraints
                List.of(),     // primary_key_constraints
                List.of(),     // foreign_key_constraints
                false,
                false,
                false,
                false,
                false,
                scanFn,
                null,
                null,
                null,
                t.inlineCardinality() ? t.cardinalityEstimate() : null,
                t.inlineCardinality() ? t.cardinalityMax() : null,
                null,
                null);
    }

    @Override
    public ItemsResponse catalog_schema_contents_views(
            byte[] attach_id, String name, byte[] transaction_id) {
        List<byte[]> items = new ArrayList<>();
        for (Worker.View v : worker.views()) {
            if (!v.schema().equals(name)) continue;
            items.add(RecordCodec.serializeToBytes(new farm.query.vgi.protocol.ViewInfo(
                    v.comment(), v.tags(), v.name(), v.schema(), v.definition())));
        }
        return new ItemsResponse(items);
    }

    @Override
    public ItemsResponse catalog_view_get(byte[] attach_id, String schema_name, String name, byte[] transaction_id) {
        for (Worker.View v : worker.views()) {
            if (v.schema().equals(schema_name) && v.name().equals(name)) {
                return new ItemsResponse(List.of(RecordCodec.serializeToBytes(
                        new farm.query.vgi.protocol.ViewInfo(v.comment(), v.tags(),
                                v.name(), v.schema(), v.definition()))));
            }
        }
        return ItemsResponse.empty();
    }

    @Override
    public ItemsResponse catalog_schema_contents_macros(
            byte[] attach_id, String name, String type, byte[] transaction_id) {
        boolean wantScalar = type == null || type.equalsIgnoreCase("scalar")
                || type.equalsIgnoreCase("scalar_macro");
        boolean wantTable = type == null || type.equalsIgnoreCase("table")
                || type.equalsIgnoreCase("table_macro");
        List<byte[]> items = new ArrayList<>();
        for (Worker.Macro m : worker.macros()) {
            if (!m.schema().equals(name)) continue;
            boolean isScalar = m.macroType() == Worker.MacroType.SCALAR;
            if (isScalar && !wantScalar) continue;
            if (!isScalar && !wantTable) continue;
            items.add(MacroInfoSerializer.serialize(toMacroInfo(m)));
        }
        return new ItemsResponse(items);
    }

    @Override
    public ItemsResponse catalog_macro_get(byte[] attach_id, String schema_name, String name, byte[] transaction_id) {
        for (Worker.Macro m : worker.macros()) {
            if (m.schema().equals(schema_name) && m.name().equals(name)) {
                return new ItemsResponse(List.of(MacroInfoSerializer.serialize(toMacroInfo(m))));
            }
        }
        return ItemsResponse.empty();
    }

    private static farm.query.vgi.protocol.MacroInfo toMacroInfo(Worker.Macro m) {
        String macroType = m.macroType() == Worker.MacroType.SCALAR ? "scalar" : "table";
        byte[] defaults = MacroDefaultsEncoder.encode(m.parameterDefaults());
        return new farm.query.vgi.protocol.MacroInfo(
                m.comment(), m.tags(), m.name(), m.schema(), macroType,
                m.parameters(), defaults, m.definition());
    }

    @Override
    public ItemsResponse catalog_schema_contents_functions(
            byte[] attach_id, String name, String type, byte[] transaction_id) {
        if (!worker.defaultSchema().equals(name)) return ItemsResponse.empty();
        boolean wantScalar = type == null
                || type.equalsIgnoreCase("scalar")
                || type.equalsIgnoreCase("SCALAR_FUNCTION");
        boolean wantTable = type == null
                || type.equalsIgnoreCase("table")
                || type.equalsIgnoreCase("TABLE_FUNCTION");
        boolean wantAggregate = type == null
                || type.equalsIgnoreCase("aggregate")
                || type.equalsIgnoreCase("AGGREGATE_FUNCTION");
        List<byte[]> items = new ArrayList<>();
        if (wantScalar) {
            for (List<ScalarFunction> variants : scalars.values()) {
                for (ScalarFunction fn : variants) {
                    items.add(FunctionInfoSerializer.serialize(toScalarFunctionInfo(fn, name)));
                }
            }
        }
        if (wantTable) {
            for (List<TableFunction> variants : tables.values()) {
                for (TableFunction fn : variants) {
                    items.add(FunctionInfoSerializer.serialize(toTableFunctionInfo(fn, name)));
                }
            }
            // Table-in-out functions also register as function_type='table'
            // (DuckDB doesn't distinguish them at the catalog level — both
            // come back as TableFunction nodes; the bind/init flow tells them
            // apart by argument shape).
            for (List<TableInOutFunction> variants : tableInOuts.values()) {
                for (TableInOutFunction fn : variants) {
                    items.add(FunctionInfoSerializer.serialize(toTableInOutFunctionInfo(fn, name)));
                }
            }
        }
        if (wantAggregate) {
            for (List<AggregateFunction<?>> variants : aggregates.values()) {
                for (AggregateFunction<?> fn : variants) {
                    items.add(FunctionInfoSerializer.serialize(toAggregateFunctionInfo(fn, name)));
                }
            }
        }
        return new ItemsResponse(items);
    }

    private FunctionInfo toScalarFunctionInfo(ScalarFunction fn, String schemaName) {
        FunctionMetadata md = fn.metadata();
        BindResponse r = fn.onBind(new ScalarBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        return baseFunctionInfo(fn.name(), md, schemaName, "scalar",
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()),
                r.output_schema() != null ? r.output_schema() : new byte[0]);
    }

    private FunctionInfo toAggregateFunctionInfo(AggregateFunction<?> fn, String schemaName) {
        FunctionMetadata md = fn.metadata();
        byte[] outputSchemaIpc = SchemaUtil.serializeSchema(fn.outputSchema());
        return baseFunctionInfo(fn.name(), md, schemaName, "aggregate",
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()),
                outputSchemaIpc);
    }

    private FunctionInfo toTableFunctionInfo(TableFunction fn, String schemaName) {
        FunctionMetadata md = fn.metadata();
        BindResponse r = fn.onBind(new TableBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        return baseFunctionInfo(fn.name(), md, schemaName, "table",
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()),
                r.output_schema() != null ? r.output_schema() : new byte[0]);
    }

    private FunctionInfo toTableInOutFunctionInfo(TableInOutFunction fn, String schemaName) {
        FunctionMetadata md = fn.metadata();
        BindResponse r = fn.onBind(new TableInOutBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        return baseFunctionInfo(fn.name(), md, schemaName, "table",
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()),
                r.output_schema() != null ? r.output_schema() : new byte[0],
                fn.hasFinalize());
    }

    private FunctionInfo baseFunctionInfo(String name, FunctionMetadata md, String schemaName,
                                           String type, byte[] arguments, byte[] outputSchema) {
        return baseFunctionInfo(name, md, schemaName, type, arguments, outputSchema, false);
    }

    private FunctionInfo baseFunctionInfo(String name, FunctionMetadata md, String schemaName,
                                           String type, byte[] arguments, byte[] outputSchema,
                                           boolean hasFinalize) {
        return new FunctionInfo(
                md.description().isEmpty() ? null : md.description(),
                Map.of(),
                name,
                schemaName,
                type,
                arguments,
                outputSchema,
                stabilityWire(md.stability()),
                nullHandlingWire(md.nullHandling()),
                md.description(),
                List.of(),
                md.categories() == null ? List.of() : md.categories(),
                md.projectionPushdown(),
                md.filterPushdown() ? Boolean.TRUE : null,
                md.samplingPushdown() ? Boolean.TRUE : null,
                List.of(),
                null,
                1,
                "NOT_ORDER_DEPENDENT",
                "NOT_DISTINCT_DEPENDENT",
                false,
                false,  // streaming_partitioned
                hasFinalize,
                List.of(),
                List.of());
    }

    private static String stabilityWire(farm.query.vgi.function.Stability s) {
        return switch (s) {
            case CONSISTENT -> "CONSISTENT";
            case VOLATILE -> "VOLATILE";
            case CONSISTENT_WITHIN_QUERY -> "CONSISTENT_WITHIN_QUERY";
        };
    }

    private static String nullHandlingWire(farm.query.vgi.function.NullHandling n) {
        return switch (n) {
            case DEFAULT -> "DEFAULT";
            case SPECIAL -> "SPECIAL";
        };
    }
}
