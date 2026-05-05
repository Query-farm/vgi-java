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
        if (variants == null || variants.isEmpty()) return null;
        if (variants.size() == 1) return variants.get(0);
        for (T v : variants) {
            int n = countArgs(v);
            if (n == argCount) return v;
        }
        return variants.get(0);
    }

    private static int countArgs(Object fn) {
        if (fn instanceof ScalarFunction f) return f.argumentSpecs().size();
        if (fn instanceof TableFunction f) return f.argumentSpecs().size();
        if (fn instanceof TableInOutFunction f) return f.argumentSpecs().size();
        return -1;
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
            ScalarFunction fn = pickVariant(scalars.get(name), argCount);
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
            TableFunction fn = pickVariant(tables.get(name), argCount);
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
            TableInOutFunction fn = pickVariant(tableInOuts.get(name), argCount);
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
        BoundEntry bound;
        if (request.bind_opaque_data() != null) {
            String key = bytesKey(request.bind_opaque_data());
            // Don't remove — the FINALIZE-phase init re-uses the same token
            // from the INPUT-phase bind. Memory is bounded by query lifetime;
            // a per-attach GC sweep on detach is the proper fix.
            bound = pendingBinds.get(key);
            if (bound == null) {
                throw new IllegalStateException("init called with unknown bind_opaque_data token");
            }
        } else {
            // Fallback: legacy clients that don't echo opaque_data. Pick the
            // single pending bind if there is exactly one.
            if (pendingBinds.size() != 1) {
                throw new IllegalStateException("init missing bind_opaque_data and pendingBinds size = "
                        + pendingBinds.size());
            }
            String onlyKey = pendingBinds.keySet().iterator().next();
            bound = pendingBinds.get(onlyKey);
        }
        Schema realOutputSchema = SchemaUtil.deserializeSchema(request.output_schema());
        byte[] execId = request.execution_id() != null ? request.execution_id() : newExecutionId();
        GlobalInitResponse header = GlobalInitResponse.of(execId);

        if (bound instanceof BoundScalar bs) {
            Schema inputSchema = bs.inputSchema() != null ? bs.inputSchema() : new Schema(List.of());
            ScalarStreamState state = new ScalarStreamState(
                    bs.fn().name(), bs.fn().argumentSpecs().size(), request.output_schema(),
                    bs.argumentsIpc(), bs.settingsIpc());
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
                    request.tablesample_seed());
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
    public long table_function_cardinality(CardinalityRequest request) {
        // Phase 4 stub: -1 advertises "unknown cardinality" — DuckDB plans
        // accordingly. Real cardinality estimation arrives in Phase 5a.
        return -1L;
    }

    // -----------------------------------------------------------------------
    // Aggregate function lifecycle
    // -----------------------------------------------------------------------

    @Override
    public AggregateBindResponse aggregate_bind(AggregateBindRequest request) {
        return aggregateRunner.bind(request.function_name(), request.input_schema());
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
        SchemaInfo info = new SchemaInfo(null, Map.of(), attach_id, worker.defaultSchema());
        return new ItemsResponse(List.of(RecordCodec.serializeToBytes(info)));
    }

    @Override
    public ItemsResponse catalog_schema_get(byte[] attach_id, String name, byte[] transaction_id) {
        if (!worker.defaultSchema().equals(name)) return ItemsResponse.empty();
        SchemaInfo info = new SchemaInfo(null, Map.of(), attach_id, name);
        return new ItemsResponse(List.of(RecordCodec.serializeToBytes(info)));
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
                List.of(),
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
