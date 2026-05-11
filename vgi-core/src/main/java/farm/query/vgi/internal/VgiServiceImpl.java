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
                                byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc,
                                byte[] secrets)
            implements BoundEntry {}

    private record BoundTable(TableFunction fn, Arguments args, Schema inputSchema,
                               Schema outputSchema, Map<String, Object> settings,
                               byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc,
                               byte[] secrets, byte[] attachId)
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
        ServiceLocator.setCurrent(new ServiceLocator(this.scalars));
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
            BindResponse upstream = fn.onBind(new ScalarBindParams(name, args, inputSchema, settings,
                    request.secrets(), request.resolved_secrets_provided()));
            Schema outputSchema = upstream.output_schema() == null
                    ? null
                    : SchemaUtil.deserializeSchema(upstream.output_schema());
            BoundScalar bs = new BoundScalar(fn, args, inputSchema, outputSchema, settings,
                    request.arguments(), request.settings(), upstream.output_schema(),
                    request.secrets());
            pendingBinds.put(key, bs);
            return new BindResponse(upstream.output_schema(), token,
                    upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
        }
        if (tables.containsKey(name)) {
            TableFunction fn = pickVariant(tables.get(name), argCount, args, inputSchema);
            BindResponse upstream = fn.onBind(new TableBindParams(name, args, inputSchema, settings,
                    request.secrets(), request.resolved_secrets_provided(), request.attach_id()));
            Schema outputSchema = upstream.output_schema() == null
                    ? null
                    : SchemaUtil.deserializeSchema(upstream.output_schema());
            BoundTable bt = new BoundTable(fn, args, inputSchema, outputSchema, settings,
                    request.arguments(), request.settings(), upstream.output_schema(),
                    request.secrets(), request.attach_id());
            pendingBinds.put(key, bt);
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
        long maxWorkers = bound instanceof BoundTable bt
                ? bt.fn().maxWorkers() : 1L;
        GlobalInitResponse header = new GlobalInitResponse(execId, maxWorkers, null);

        if (bound instanceof BoundScalar bs) {
            Schema inputSchema = bs.inputSchema() != null ? bs.inputSchema() : new Schema(List.of());
            int variantIdx = ServiceLocator.current().scalarIndexOf(bs.fn().name(), bs.fn());
            ScalarStreamState state = new ScalarStreamState(
                    bs.fn().name(), bs.fn().argumentSpecs().size(), variantIdx,
                    request.output_schema(), bs.argumentsIpc(), bs.settingsIpc());
            state.setSecrets(bs.secrets());
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
                    request.order_by_limit(),
                    execId,
                    bt.secrets(),
                    bt.attachId());
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

    private static String bytesKey(byte[] b) { return HexId.encode(b); }

    private static byte[] newExecutionId() { return HexId.newExecutionId(); }

    // -----------------------------------------------------------------------
    // Cardinality (table functions only; scalars never get this call)
    // -----------------------------------------------------------------------

    @Override
    public farm.query.vgi.protocol.CardinalityResponse table_function_cardinality(byte[] request) {
        CardinalityRequest inner = unpackCardinalityRequest(request);
        long result = computeCardinality(inner);
        return new farm.query.vgi.protocol.CardinalityResponse(
                result < 0 ? null : result,
                result < 0 ? null : result);
    }

    @Override
    public farm.query.vgi.protocol.DynamicToStringResponse table_function_dynamic_to_string(byte[] request) {
        DynamicToStringInner inner = unpackDynamicToStringRequest(request);
        if (inner == null || inner.bind_call == null) {
            return new farm.query.vgi.protocol.DynamicToStringResponse(List.of(), List.of());
        }
        BindRequest embedded = RecordCodec.deserializeFromBytes(inner.bind_call, BindRequest.class);
        TableFunction fn = null;
        if (tables.containsKey(embedded.function_name())) {
            // Pick a variant — for diagnostics any registered overload of the
            // name suffices since they share state.
            Schema inputSchema = SchemaUtil.deserializeSchema(embedded.input_schema());
            Arguments args = ArgumentsParser.parse(embedded.arguments());
            int constN = args.positional().size();
            int colN = inputSchema == null ? 0 : inputSchema.getFields().size();
            fn = pickVariant(tables.get(embedded.function_name()), constN + colN, args, inputSchema);
        }
        if (fn == null) {
            return new farm.query.vgi.protocol.DynamicToStringResponse(List.of(), List.of());
        }
        java.util.LinkedHashMap<String, String> kv = fn.dynamicToString(inner.global_execution_id);
        List<String> keys = new ArrayList<>(kv.keySet());
        List<String> values = new ArrayList<>(kv.values());
        return new farm.query.vgi.protocol.DynamicToStringResponse(keys, values);
    }

    private record DynamicToStringInner(byte[] bind_call, byte[] bind_opaque_data, byte[] global_execution_id) {}

    private static DynamicToStringInner unpackDynamicToStringRequest(byte[] request) {
        Map<String, byte[]> fields = unpackBinaryFields(request, "bind_call", "bind_opaque_data", "global_execution_id");
        if (fields == null) return null;
        return new DynamicToStringInner(fields.get("bind_call"),
                fields.get("bind_opaque_data"),
                fields.get("global_execution_id"));
    }

    /**
     * Wire shape: an IPC stream of {@code {bind_call: binary, bind_opaque_data:
     * binary?}}. Returns an empty {@link CardinalityRequest} on parse failure
     * so the per-bind fallback logic still has something to dispatch on.
     */
    private static CardinalityRequest unpackCardinalityRequest(byte[] request) {
        Map<String, byte[]> fields = unpackBinaryFields(request, "bind_call", "bind_opaque_data");
        if (fields == null) return new CardinalityRequest(null, null);
        return new CardinalityRequest(fields.get("bind_call"), fields.get("bind_opaque_data"));
    }

    /**
     * Decode a 1-row IPC stream into {@code name → byte[]} for the requested
     * VarBinary columns. Returns {@code null} on parse failure / empty batch
     * so callers can route to their own fallback. Per-cell nulls are simply
     * omitted from the returned map.
     */
    private static Map<String, byte[]> unpackBinaryFields(byte[] request, String... fieldNames) {
        if (request == null || request.length == 0) return null;
        try (java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(request);
             org.apache.arrow.vector.ipc.ArrowStreamReader reader =
                     new org.apache.arrow.vector.ipc.ArrowStreamReader(in, Allocators.root())) {
            if (!reader.loadNextBatch()) return null;
            org.apache.arrow.vector.VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (root.getRowCount() == 0) return null;
            Map<String, byte[]> out = new HashMap<>();
            for (String name : fieldNames) {
                org.apache.arrow.vector.VarBinaryVector vec =
                        (org.apache.arrow.vector.VarBinaryVector) root.getVector(name);
                if (vec != null && !vec.isNull(0)) out.put(name, vec.get(0));
            }
            return out;
        } catch (Exception e) {
            return null;
        }
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
        List<byte[]> attachOptionBytes = new ArrayList<>();
        for (farm.query.vgi.AttachOptionSpec spec : worker.attachOptionSpecs()) {
            attachOptionBytes.add(AttachOptionSpecSerializer.serialize(spec));
        }
        return new ItemsResponse(List.of(CatalogInfoSerializer.serialize(
                worker.catalogName(), worker.implementationVersion(),
                worker.dataVersionSpec(), attachOptionBytes)));
    }

    @Override
    public farm.query.vgi.protocol.CatalogVersionResponse catalog_version(byte[] attach_id, byte[] transaction_id) {
        return new farm.query.vgi.protocol.CatalogVersionResponse(1L);
    }

    /** Per-attach resolved-data-version, keyed by attach_id hex. Used by
     *  catalog_table_get to pick the right per-version table variant for
     *  information_schema queries (which don't pass at_value). */
    private final java.util.Map<String, String> attachDataVersions = new java.util.concurrent.ConcurrentHashMap<>();

    /** Per-attach catalog name (from the {@code ATTACH 'name'} clause).
     *  Used to filter fixture-vs-fixture across multiple logical catalogs
     *  served by the same worker binary. */
    private final java.util.Map<String, String> attachCatalogNames = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public CatalogAttachResult catalog_attach(CatalogAttachRequest request, CallContext ctx) {
        byte[] attachId;
        if (!worker.attachOptionSpecs().isEmpty()) {
            // attach_options pattern (Go/Python parity): encode merged
            // {defaults + user options} batch directly into attach_id so the
            // echo function is stateless under pool reuse / HTTP transport.
            // attach_id = uuid(16) || 0x00 || ipc(mergedBatch).
            attachId = AttachOptionsAttachId.encode(
                    worker.attachOptionSpecs(), request.options(), rng);
        } else {
            attachId = new byte[16];
            rng.nextBytes(attachId);
        }
        List<byte[]> settings = new ArrayList<>();
        for (SettingSpec spec : worker.settingSpecs()) {
            settings.add(SettingSpecSerializer.serialize(spec));
        }
        List<byte[]> secretTypes = new ArrayList<>();
        for (farm.query.vgi.SecretTypeSpec spec : worker.secretTypeSpecs()) {
            secretTypes.add(SecretTypeSpecSerializer.serialize(spec));
        }
        // Resolve client-supplied / default version specs. Reject specs the
        // worker can't satisfy with a clear error (the C++ extension
        // surfaces "Unsupported data_version_spec" / "...implementation_version"
        // back to ATTACH).
        String resolvedImpl = resolveImplementationVersion(request.implementation_version());
        String resolvedData = resolveDataVersion(request.data_version_spec());
        if (resolvedData != null) attachDataVersions.put(bytesKey(attachId), resolvedData);
        if (request.name() != null && !request.name().isEmpty()) {
            attachCatalogNames.put(bytesKey(attachId), request.name());
        }
        Map<String, String> tags = new java.util.LinkedHashMap<>(worker.catalogTags() == null ? Map.of() : worker.catalogTags());
        if (resolvedImpl != null) tags.put("vgi_resolved_implementation_version", resolvedImpl);
        if (resolvedData != null) tags.put("vgi_resolved_data_version", resolvedData);
        return new CatalogAttachResult(
                attachId,
                false, true, false,
                1L,
                false,
                worker.defaultSchema(),
                settings,
                secretTypes,
                worker.catalogComment(),
                tags,
                false,
                resolvedData,
                resolvedImpl);
    }

    private String resolveImplementationVersion(String requested) {
        String supported = worker.implementationVersion();
        if (supported == null) return null;
        if (requested == null || requested.isEmpty()) return supported;
        // Allow npm-style ^/~ specs for implementation_version too.
        String implVersionsList = System.getenv("VGI_WORKER_SUPPORTED_IMPL_VERSIONS");
        List<String> implVersions = implVersionsList != null && !implVersionsList.isEmpty()
                ? java.util.Arrays.asList(implVersionsList.split(","))
                : java.util.List.of(supported);
        String resolved = SemverHelpers.resolveNpmSpec(requested, implVersions);
        if (resolved == null) {
            throw new IllegalArgumentException("Unsupported implementation_version: " + requested);
        }
        return resolved;
    }

    private String resolveDataVersion(String spec) {
        // The worker's dataVersionSpec is the supported range (e.g.
        // ">=1.0.0,<2.0.0"). Caller spec accepts exact versions, npm-style
        // ^/~ specs, or partial versions ("1", "1.0"). Each is resolved to
        // a concrete version drawn from a small known set. Outside the
        // worker's supported range → error.
        String range = worker.dataVersionSpec();
        if (range == null) return null;
        if (spec == null || spec.isEmpty()) {
            String defaultVer = System.getenv("VGI_WORKER_DEFAULT_DATA_VERSION");
            return defaultVer != null && !defaultVer.isEmpty() ? defaultVer : "1.2.0";
        }
        List<String> supportedVersions = supportedVersions();
        String resolved = SemverHelpers.resolveNpmSpec(spec, supportedVersions);
        if (resolved == null || !SemverHelpers.matchesRange(resolved, range)) {
            throw new IllegalArgumentException("Unsupported data_version_spec: " + spec);
        }
        return resolved;
    }

    private List<String> supportedVersions() {
        String list = System.getenv("VGI_WORKER_SUPPORTED_VERSIONS");
        if (list != null && !list.isEmpty()) {
            return java.util.Arrays.asList(list.split(","));
        }
        // Default set covering the most common test scenarios.
        return java.util.List.of("1.0.0", "1.1.0", "1.2.0", "2.0.0", "3.0.0");
    }


    @Override
    public void catalog_detach(byte[] attach_id) {
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
    /**
     * Per-schema object counts. Keys are the C++ extension's
     * ``VgiObjectCounts`` field names (singular: ``"table"``, ``"view"``,
     * ``"macro"``, ``"index"``, ``"scalar_function"``,
     * ``"aggregate_function"``, ``"table_function"``). When a key is
     * {@code 0}, the C++ extension treats it as a hard guarantee and skips
     * the corresponding ``catalog_schema_contents_*`` RPC entirely (see
     * ``VgiCatalogSet::ShouldBypassRpcLocked``).
     */
    private Map<String, Long> schemaCounts(SchemaDesc s) {
        Map<String, Long> m = new java.util.LinkedHashMap<>();
        long scalarCount = 0;
        long tableFnCount = 0;
        long aggregateCount = 0;
        if (s.name.equals(worker.defaultSchema())) {
            for (var v : scalars.values()) scalarCount += v.size();
            for (var v : tables.values()) tableFnCount += v.size();
            for (var v : tableInOuts.values()) tableFnCount += v.size();
            for (var v : aggregates.values()) aggregateCount += v.size();
        }
        m.put("scalar_function", scalarCount);
        m.put("table_function", tableFnCount);
        m.put("aggregate_function", aggregateCount);
        long tableCount = worker.catalogTables().stream()
                .filter(t -> t.schema().equals(s.name))
                .count();
        m.put("table", tableCount);
        long viewCount = worker.views().stream()
                .filter(v -> v.schema().equals(s.name))
                .count();
        m.put("view", viewCount);
        long macroCount = worker.macros().stream()
                .filter(macro -> macro.schema().equals(s.name))
                .count();
        m.put("macro", macroCount);
        m.put("index", 0L);
        return m;
    }

    @Override
    public ItemsResponse catalog_schema_contents_tables(
            byte[] attach_id, String name, byte[] transaction_id) {
        List<byte[]> items = new ArrayList<>();
        String dv = attach_id == null ? null : attachDataVersions.get(bytesKey(attach_id));
        for (Worker.CatalogTable t : worker.catalogTables()) {
            if (!t.schema().equals(name)) continue;
            // Hide per-version variant tables (e.g. versioned_data_v1,
            // animals_v_1_0_0) from table listings — they are dispatched
            // through the un-suffixed table via the AT (VERSION => ...)
            // clause or via the attach's resolved data version.
            if (t.name().matches(".*_v\\d+$")) continue;
            if (t.name().matches(".*_v(_\\d+){3,}$")) continue;
            // Catalog isolation. When the worker acts as the versioned-tables
            // catalog, only the animals/plants user-visible fixtures are
            // exposed (and only ones available at the resolved data version).
            boolean isVersionedTables = "versioned_tables".equals(worker.catalogName());
            boolean isVtFixture = "animals".equals(t.name()) || "plants".equals(t.name());
            if (isVersionedTables && !isVtFixture) continue;
            if (!isVersionedTables && isVtFixture) continue;
            if (isVersionedTables && "plants".equals(t.name()) && dv != null
                    && SemverHelpers.compareVersions(dv, "2.0.0") < 0) continue;
            if (isVersionedTables && "animals".equals(t.name()) && dv != null
                    && SemverHelpers.compareVersions(dv, "3.0.0") >= 0) continue;
            Worker.CatalogTable resolved = dv == null ? t : resolveVersion(t, "data_version", dv);
            items.add(TableInfoSerializer.serialize(toTableInfo(resolved)));
        }
        return new ItemsResponse(items);
    }

    @Override
    public farm.query.vgi.protocol.TableScanFunctionGetResponse catalog_table_scan_function_get(
            byte[] attach_id, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_id) {
        // Same version-resolution logic as catalog_table_get: if the table
        // has per-version variants and the caller didn't explicitly ask for
        // a version, fall back to the attach's resolved data version.
        String effectiveAtUnit = at_unit;
        String effectiveAtValue = at_value;
        if ((effectiveAtUnit == null || effectiveAtUnit.isEmpty()) && attach_id != null) {
            String dv = attachDataVersions.get(bytesKey(attach_id));
            if (dv != null) {
                effectiveAtUnit = "data_version";
                effectiveAtValue = dv;
            }
        }
        for (Worker.CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                Worker.CatalogTable resolved = resolveVersion(t, effectiveAtUnit, effectiveAtValue);
                if (resolved.scanFunctionName() == null) break;
                byte[] argsBytes = ScanFunctionResultEncoder.encodeArguments(
                        resolved.scanFunctionPositional() == null ? List.of() : resolved.scanFunctionPositional(),
                        resolved.scanFunctionNamed() == null ? Map.of() : resolved.scanFunctionNamed());
                return new farm.query.vgi.protocol.TableScanFunctionGetResponse(
                        resolved.scanFunctionName(), argsBytes, List.of());
            }
        }
        throw new IllegalArgumentException("scan_function_get: unknown table " + schema_name + "." + name);
    }

    @Override
    public ItemsResponse catalog_table_get(
            byte[] attach_id, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_id) {
        // If the table has per-version variants and the caller didn't pass
        // at_unit/at_value, default to the version this attach was bound to
        // (so information_schema queries pick the right schema).
        String effectiveAtUnit = at_unit;
        String effectiveAtValue = at_value;
        String dv = attach_id == null ? null : attachDataVersions.get(bytesKey(attach_id));
        if ((effectiveAtUnit == null || effectiveAtUnit.isEmpty()) && dv != null) {
            effectiveAtUnit = "data_version";
            effectiveAtValue = dv;
        }
        // Hide plants below 2.0.0 in the versioned-tables catalog.
        if ("versioned_tables".equals(worker.catalogName())) {
            if ("plants".equals(name) && dv != null && SemverHelpers.compareVersions(dv, "2.0.0") < 0) {
                return ItemsResponse.empty();
            }
            if ("animals".equals(name) && dv != null && SemverHelpers.compareVersions(dv, "3.0.0") >= 0) {
                return ItemsResponse.empty();
            }
        }
        for (Worker.CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                Worker.CatalogTable resolved = resolveVersion(t, effectiveAtUnit, effectiveAtValue);
                return new ItemsResponse(List.of(TableInfoSerializer.serialize(toTableInfo(resolved))));
            }
        }
        return ItemsResponse.empty();
    }

    /**
     * For {@code versioned_data} / {@code versioned_constraints}, swap the
     * declared columns + scan args for a version-specific variant when the
     * client asked {@code AT (VERSION => N)}. Other tables and AT clauses
     * pass through unchanged. Throws if N is out of range so DuckDB surfaces
     * "Unknown version" / "table did not exist before <year>" cleanly.
     */
    private Worker.CatalogTable resolveVersion(Worker.CatalogTable t, String at_unit, String at_value) {
        if (at_unit == null || at_value == null || at_unit.isEmpty()) return t;
        if ("data_version".equalsIgnoreCase(at_unit)) {
            // Try a registered "<name>_v_<X>_<Y>_<Z>" variant — used by
            // version-aware catalog tables that aren't time-travel tables.
            String suffix = "_v_" + at_value.replace('.', '_');
            String dvName = t.name() + suffix;
            for (Worker.CatalogTable vt : worker.catalogTables()) {
                if (vt.schema().equals(t.schema()) && vt.name().equals(dvName)) {
                    return new Worker.CatalogTable(
                            t.schema(), t.name(), vt.columns(), t.comment(), t.tags(),
                            vt.scanFunctionName(), vt.scanFunctionPositional(), vt.scanFunctionNamed(),
                            null, null, false, true,
                            List.of(), List.of(), List.of(), List.of());
                }
            }
            return t;
        }
        // Tables that don't declare time-travel behaviour reject AT clauses
        // up-front. The catalog-level supports_time_travel=true gate is
        // necessary for time-travel-capable tables to be accepted by
        // DuckDB's binder, but each individual table can still error out
        // here when asked for a non-default version.
        if (!"versioned_data".equals(t.name()) && !"versioned_constraints".equals(t.name())) {
            throw new IllegalArgumentException("table " + t.name() + " does not support time travel");
        }
        int version = -1;
        if ("version".equalsIgnoreCase(at_unit)) {
            try { version = Integer.parseInt(at_value); }
            catch (NumberFormatException e) { throw new IllegalArgumentException("Unknown version: " + at_value); }
        } else if ("timestamp".equalsIgnoreCase(at_unit)) {
            // Map the timestamp to a version: <2020 errors, [2020..2021) → v1,
            // [2021..2022) → v2, ≥2022 → v3 (matches time_travel.test).
            String s = at_value;
            int yearEnd = Math.min(4, s.length());
            int year;
            try { year = Integer.parseInt(s.substring(0, yearEnd)); }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Unknown timestamp: " + at_value);
            }
            if (year < 2020) {
                throw new IllegalArgumentException("table did not exist before 2020");
            }
            if (year < 2021) version = 1;
            else if (year < 2022) version = 2;
            else version = 3;
        } else {
            return t;
        }
        if (version < 1 || version > 3) {
            throw new IllegalArgumentException("Unknown version: " + version);
        }
        // Find the registered CatalogTable for "<name>_v<version>" and reuse
        // its columns + scan args. The worker registers per-version variants
        // as hidden internal tables; this method swaps them in at query time.
        // Constraints are dropped on the versioned variant because column
        // indices baked into PK/UNIQUE/FK refer to the *default* schema and
        // would point past the end on older versions with fewer columns.
        String versionedName = t.name() + "_v" + version;
        for (Worker.CatalogTable vt : worker.catalogTables()) {
            if (vt.schema().equals(t.schema()) && vt.name().equals(versionedName)) {
                return new Worker.CatalogTable(
                        t.schema(), t.name(), vt.columns(), t.comment(), t.tags(),
                        vt.scanFunctionName(), vt.scanFunctionPositional(), vt.scanFunctionNamed(),
                        null, null, false, true,
                        List.of(), List.of(), List.of(), List.of());
            }
        }
        return t;
    }

    private static List<Integer> extractNotNullColumnIndices(byte[] columnsIpc) {
        if (columnsIpc == null || columnsIpc.length == 0) return List.of();
        org.apache.arrow.vector.types.pojo.Schema s = SchemaUtil.deserializeSchema(columnsIpc);
        if (s == null) return List.of();
        java.util.List<org.apache.arrow.vector.types.pojo.Field> fields = s.getFields();
        java.util.ArrayList<Integer> out = new java.util.ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            if (!fields.get(i).isNullable()) out.add(i);
        }
        return out;
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
        List<Integer> notNullCols = extractNotNullColumnIndices(t.columns());
        List<byte[]> foreignKeys = new ArrayList<>();
        if (t.foreignKeys() != null) {
            for (Worker.CatalogTable.ForeignKey fk : t.foreignKeys()) {
                foreignKeys.add(ForeignKeySerializer.serialize(fk));
            }
        }
        return new farm.query.vgi.protocol.TableInfo(
                t.comment() == null || t.comment().isEmpty() ? null : t.comment(),
                t.tags() == null ? Map.of() : t.tags(),
                t.name(),
                t.schema(),
                t.columns() == null ? new byte[0] : t.columns(),
                notNullCols,
                t.uniqueConstraints() == null ? List.of() : t.uniqueConstraints(),
                t.checkConstraints() == null ? List.of() : t.checkConstraints(),
                t.primaryKey() == null ? List.of() : t.primaryKey(),
                foreignKeys,
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
        // Versioned-tables catalog ships no user-visible views.
        if ("versioned_tables".equals(worker.catalogName())) {
            return new ItemsResponse(items);
        }
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
            String attachCatName = attach_id == null ? null : attachCatalogNames.get(bytesKey(attach_id));
            boolean isProjReproAttach = "projection_repro".equals(attachCatName);
            for (List<TableFunction> variants : tables.values()) {
                for (TableFunction fn : variants) {
                    // proj_repro_* fixtures live in the example worker binary
                    // but only belong to the projection_repro catalog. Show
                    // them only when that catalog was attached.
                    if (!isProjReproAttach && fn.name().startsWith("proj_repro_")) continue;
                    if (isProjReproAttach && !fn.name().startsWith("proj_repro_")) continue;
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
                md.orderPreservation() == null ? null : md.orderPreservation().name(),
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
