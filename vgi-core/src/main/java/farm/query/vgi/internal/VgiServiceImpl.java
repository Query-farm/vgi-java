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
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.View;

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
     *
     * <p>Bounded to avoid unbounded growth when bind/init isn't followed by
     * cleanup (cancelled query plans, statistics fan-out). Entries are
     * normally short-lived; the cap is sized for the worst-case planner fan-out.</p>
     */
    private static final int MAX_PENDING_BINDS = 4096;
    private static final int MAX_TIO_EXECUTIONS = 1024;
    private final java.util.Map<String, BoundEntry> pendingBinds = BoundedMap.create(MAX_PENDING_BINDS);
    /**
     * Per-execution table-in-out exchange states, keyed by hex execution_id.
     * Survives the INPUT→FINALIZE phase boundary so that buffered state
     * accumulated during exchange ticks can be drained at finalize time.
     */
    private final java.util.Map<String, TioExecutionState> tioExecutions = BoundedMap.create(MAX_TIO_EXECUTIONS);

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
        this.catalogRegistry = new CatalogRegistry(worker);
        ServiceLocator.setCurrent(new ServiceLocator(this.scalars));
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

        byte[] token = new byte[16];
        rng.nextBytes(token);

        int argCount = args.positional().size()
                + (inputSchema == null ? 0 : inputSchema.getFields().size());

        if (scalars.containsKey(name)) {
            return bindScalar(request, name, args, inputSchema, settings, argCount, token);
        }
        if (tables.containsKey(name)) {
            return bindTable(request, name, args, inputSchema, settings, argCount, token);
        }
        if (tableInOuts.containsKey(name)) {
            return bindTableInOut(request, name, args, inputSchema, settings, argCount, token);
        }
        throw new IllegalArgumentException("Unknown function: " + name);
    }

    private BindResponse bindScalar(BindRequest request, String name, Arguments args,
                                     Schema inputSchema, Map<String, Object> settings,
                                     int argCount, byte[] token) {
        ScalarFunction fn = OverloadResolver.pick(scalars.get(name), argCount, args, inputSchema);
        BindResponse upstream = fn.onBind(new ScalarBindParams(name, args, inputSchema, settings,
                request.secrets(), request.resolved_secrets_provided()));
        Schema outputSchema = upstream.output_schema() == null
                ? null : SchemaUtil.deserializeSchema(upstream.output_schema());
        pendingBinds.put(bytesKey(token), new BoundScalar(fn, args, inputSchema, outputSchema, settings,
                request.arguments(), request.settings(), upstream.output_schema(),
                request.secrets()));
        return new BindResponse(upstream.output_schema(), token,
                upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
    }

    private BindResponse bindTable(BindRequest request, String name, Arguments args,
                                    Schema inputSchema, Map<String, Object> settings,
                                    int argCount, byte[] token) {
        TableFunction fn = OverloadResolver.pick(tables.get(name), argCount, args, inputSchema);
        BindResponse upstream = fn.onBind(new TableBindParams(name, args, inputSchema, settings,
                request.secrets(), request.resolved_secrets_provided(), request.attach_id()));
        Schema outputSchema = upstream.output_schema() == null
                ? null : SchemaUtil.deserializeSchema(upstream.output_schema());
        pendingBinds.put(bytesKey(token), new BoundTable(fn, args, inputSchema, outputSchema, settings,
                request.arguments(), request.settings(), upstream.output_schema(),
                request.secrets(), request.attach_id()));
        return new BindResponse(upstream.output_schema(), token,
                upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
    }

    private BindResponse bindTableInOut(BindRequest request, String name, Arguments args,
                                         Schema inputSchema, Map<String, Object> settings,
                                         int argCount, byte[] token) {
        TableInOutFunction fn = OverloadResolver.pick(tableInOuts.get(name), argCount, args, inputSchema);
        BindResponse upstream = fn.onBind(new TableInOutBindParams(name, args, inputSchema, settings));
        Schema outputSchema = upstream.output_schema() == null
                ? null : SchemaUtil.deserializeSchema(upstream.output_schema());
        pendingBinds.put(bytesKey(token), new BoundTableInOut(fn, args, inputSchema, outputSchema, settings,
                request.arguments(), request.settings(), upstream.output_schema()));
        return new BindResponse(upstream.output_schema(), token,
                upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
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

        if (bound instanceof BoundScalar bs) return initScalar(request, bs, realOutputSchema, header);
        if (bound instanceof BoundTable bt) return initTable(request, bt, realOutputSchema, execId, header);
        if (bound instanceof BoundTableInOut bio) return initTableInOut(request, bio, realOutputSchema, execId, header);
        throw new IllegalStateException("Unexpected bound type: " + bound);
    }

    private RpcStream<? extends StreamState> initScalar(InitRequest request, BoundScalar bs,
                                                          Schema realOutputSchema, GlobalInitResponse header) {
        Schema inputSchema = bs.inputSchema() != null ? bs.inputSchema() : new Schema(List.of());
        int variantIdx = ServiceLocator.current().scalarIndexOf(bs.fn().name(), bs.fn());
        ScalarStreamState state = new ScalarStreamState(
                bs.fn().name(), bs.fn().argumentSpecs().size(), variantIdx,
                request.output_schema(), bs.argumentsIpc(), bs.settingsIpc());
        state.setSecrets(bs.secrets());
        return RpcStream.exchange(inputSchema, realOutputSchema, state, header);
    }

    private RpcStream<? extends StreamState> initTable(InitRequest request, BoundTable bt,
                                                         Schema realOutputSchema, byte[] execId,
                                                         GlobalInitResponse header) {
        // Project the full output schema down to the columns DuckDB requested,
        // but only when the function opts into projection pushdown. Otherwise
        // the framework would have to auto-project emitted batches (not
        // implemented), so non-pushdown functions keep the full schema.
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

    private RpcStream<? extends StreamState> initTableInOut(InitRequest request, BoundTableInOut bio,
                                                              Schema realOutputSchema, byte[] execId,
                                                              GlobalInitResponse header) {
        String phase = request.phase();
        String execKey = bytesKey(execId);
        if ("FINALIZE".equalsIgnoreCase(phase)) {
            // Look up the exchange state captured during INPUT phase and
            // emit buffered batches via a producer stream.
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
        Schema inputSchema = bio.inputSchema() != null ? bio.inputSchema() : new Schema(List.of());
        TableInOutInitParams params = new TableInOutInitParams(
                bio.fn().name(), bio.args(), inputSchema, realOutputSchema,
                bio.settings(), Allocators.root());
        TableInOutExchangeState state = bio.fn().createExchange(params);
        if (bio.fn().hasFinalize()) {
            tioExecutions.put(execKey, new TioExecutionState(bio.fn(), state, params));
        }
        return RpcStream.exchange(inputSchema, realOutputSchema, state, header);
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
        Map<String, byte[]> fields = IpcUnpacker.unpack(request, "bind_call", "bind_opaque_data");
        CardinalityRequest inner = fields == null
                ? new CardinalityRequest(null, null)
                : new CardinalityRequest(fields.get("bind_call"), fields.get("bind_opaque_data"));
        long result = computeCardinality(inner);
        return new farm.query.vgi.protocol.CardinalityResponse(
                result < 0 ? null : result,
                result < 0 ? null : result);
    }

    @Override
    public farm.query.vgi.protocol.DynamicToStringResponse table_function_dynamic_to_string(byte[] request) {
        Map<String, byte[]> fields = IpcUnpacker.unpack(request,
                "bind_call", "bind_opaque_data", "global_execution_id");
        byte[] bindCall = fields == null ? null : fields.get("bind_call");
        if (bindCall == null) {
            return new farm.query.vgi.protocol.DynamicToStringResponse(List.of(), List.of());
        }
        DynamicToStringInner inner = new DynamicToStringInner(bindCall,
                fields.get("bind_opaque_data"), fields.get("global_execution_id"));
        BindRequest embedded = RecordCodec.deserializeFromBytes(inner.bind_call, BindRequest.class);
        TableFunction fn = null;
        if (tables.containsKey(embedded.function_name())) {
            // Pick a variant — for diagnostics any registered overload of the
            // name suffices since they share state.
            Schema inputSchema = SchemaUtil.deserializeSchema(embedded.input_schema());
            Arguments args = ArgumentsParser.parse(embedded.arguments());
            int constN = args.positional().size();
            int colN = inputSchema == null ? 0 : inputSchema.getFields().size();
            fn = OverloadResolver.pick(tables.get(embedded.function_name()), constN + colN, args, inputSchema);
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
                TableFunction fn = OverloadResolver.pick(tables.get(embedded.function_name()),
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

    private final CatalogRegistry catalogRegistry;

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
        catalogRegistry.recordAttach(attachId, resolvedData, request.name());
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
        String dv = catalogRegistry.dataVersion(attach_id);
        boolean isVersionedTables = "versioned_tables".equals(worker.catalogName());
        for (CatalogTable t : worker.catalogTables()) {
            if (!t.schema().equals(name)) continue;
            // Hide per-version variant tables (e.g. versioned_data_v1,
            // animals_v_1_0_0) from table listings — they are dispatched
            // through the un-suffixed table via the AT (VERSION => ...)
            // clause or via the attach's resolved data version.
            if (t.name().matches(".*_v\\d+$")) continue;
            if (t.name().matches(".*_v(_\\d+){3,}$")) continue;
            // Catalog isolation: versioned_tables only exposes animals/plants;
            // other catalogs must not expose them.
            boolean isVtFixture = "animals".equals(t.name()) || "plants".equals(t.name());
            if (isVersionedTables != isVtFixture) continue;
            if (catalogRegistry.isHiddenInVersionedTables(t.name(), attach_id)) continue;
            CatalogTable resolved = dv == null ? t : catalogRegistry.resolveVersion(t, "data_version", dv);
            items.add(TableInfoSerializer.serialize(toTableInfo(resolved)));
        }
        return new ItemsResponse(items);
    }

    @Override
    public farm.query.vgi.protocol.TableScanFunctionGetResponse catalog_table_scan_function_get(
            byte[] attach_id, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_id) {
        var at = catalogRegistry.effectiveAt(attach_id, at_unit, at_value);
        for (CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                CatalogTable resolved = catalogRegistry.resolveVersion(t, at.unit(), at.value());
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
        if (catalogRegistry.isHiddenInVersionedTables(name, attach_id)) return ItemsResponse.empty();
        var at = catalogRegistry.effectiveAt(attach_id, at_unit, at_value);
        for (CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                CatalogTable resolved = catalogRegistry.resolveVersion(t, at.unit(), at.value());
                return new ItemsResponse(List.of(TableInfoSerializer.serialize(toTableInfo(resolved))));
            }
        }
        return ItemsResponse.empty();
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

    private farm.query.vgi.protocol.TableInfo toTableInfo(CatalogTable t) {
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
            for (CatalogTable.ForeignKey fk : t.foreignKeys()) {
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
        for (View v : worker.views()) {
            if (!v.schema().equals(name)) continue;
            items.add(RecordCodec.serializeToBytes(new farm.query.vgi.protocol.ViewInfo(
                    v.comment(), v.tags(), v.name(), v.schema(), v.definition())));
        }
        return new ItemsResponse(items);
    }

    @Override
    public ItemsResponse catalog_view_get(byte[] attach_id, String schema_name, String name, byte[] transaction_id) {
        for (View v : worker.views()) {
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
        for (Macro m : worker.macros()) {
            if (!m.schema().equals(name)) continue;
            boolean isScalar = m.macroType() == MacroType.SCALAR;
            if (isScalar && !wantScalar) continue;
            if (!isScalar && !wantTable) continue;
            items.add(MacroInfoSerializer.serialize(toMacroInfo(m)));
        }
        return new ItemsResponse(items);
    }

    @Override
    public ItemsResponse catalog_macro_get(byte[] attach_id, String schema_name, String name, byte[] transaction_id) {
        for (Macro m : worker.macros()) {
            if (m.schema().equals(schema_name) && m.name().equals(name)) {
                return new ItemsResponse(List.of(MacroInfoSerializer.serialize(toMacroInfo(m))));
            }
        }
        return ItemsResponse.empty();
    }

    private static farm.query.vgi.protocol.MacroInfo toMacroInfo(Macro m) {
        String macroType = m.macroType() == MacroType.SCALAR ? "scalar" : "table";
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
            String attachCatName = catalogRegistry.catalogName(attach_id);
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
        BindResponse r = fn.onBind(new ScalarBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        return baseFunctionInfo(fn, schemaName, "scalar", bindOutput(r), false);
    }

    private FunctionInfo toTableFunctionInfo(TableFunction fn, String schemaName) {
        BindResponse r = fn.onBind(new TableBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        return baseFunctionInfo(fn, schemaName, "table", bindOutput(r), false);
    }

    private FunctionInfo toTableInOutFunctionInfo(TableInOutFunction fn, String schemaName) {
        BindResponse r = fn.onBind(new TableInOutBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        return baseFunctionInfo(fn, schemaName, "table", bindOutput(r), fn.hasFinalize());
    }

    private FunctionInfo toAggregateFunctionInfo(AggregateFunction<?> fn, String schemaName) {
        return baseFunctionInfo(fn, schemaName, "aggregate",
                SchemaUtil.serializeSchema(fn.outputSchema()), false);
    }

    private static byte[] bindOutput(BindResponse r) {
        return r.output_schema() != null ? r.output_schema() : new byte[0];
    }

    private FunctionInfo baseFunctionInfo(farm.query.vgi.function.FunctionDescriptor fn,
                                           String schemaName, String type,
                                           byte[] outputSchema, boolean hasFinalize) {
        return baseFunctionInfo(fn.name(), fn.metadata(), schemaName, type,
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()), outputSchema, hasFinalize);
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
