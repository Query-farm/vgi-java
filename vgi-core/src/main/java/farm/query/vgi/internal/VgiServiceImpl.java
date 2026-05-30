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
import farm.query.vgirpc.AuthContext;
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
    private final Map<String, List<farm.query.vgi.buffering.TableBufferingFunction>> bufferingFns =
            new HashMap<>();
    /**
     * The shared-state backend for this worker, selected at startup by
     * {@code VGI_WORKER_SHARED_STORAGE} (in-process {@code :memory:}, local
     * file, or a Cloudflare Durable Object). Backs table-buffering logs,
     * transaction key/value state, and aggregate group state alike.
     */
    private final farm.query.vgi.storage.FunctionStorage storage =
            farm.query.vgi.storage.StorageResolver.fromEnv();
    private final TransactionStore transactionStore = new TransactionStore(storage);
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

    /**
     * HTTP-only AEAD sealing of attach / transaction opaque data. Pure
     * passthrough on stdio / AF_UNIX (the transports the integration suite
     * uses), so the seal/unseal calls below are identity there.
     */
    private final OpaqueDataSealer sealer;

    private static AuthContext authOf(CallContext ctx) {
        return ctx == null ? AuthContext.ANONYMOUS : ctx.auth();
    }

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
                               byte[] secrets, byte[] attachId, byte[] bindOpaqueData)
            implements BoundEntry {}

    private record BoundTableInOut(TableInOutFunction fn, Arguments args, Schema inputSchema,
                                    Schema outputSchema, Map<String, Object> settings,
                                    byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc)
            implements BoundEntry {}

    private record BoundBuffering(farm.query.vgi.buffering.TableBufferingFunction fn, Arguments args,
                                   Schema inputSchema, Schema outputSchema, Map<String, Object> settings,
                                   byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc,
                                   byte[] attachId, byte[] bindOpaqueData)
            implements BoundEntry {}

    public VgiServiceImpl(Worker worker, List<ScalarFunction> scalars, List<TableFunction> tables,
                           List<TableInOutFunction> tableInOuts, List<AggregateFunction<?>> aggregates) {
        this(worker, scalars, tables, tableInOuts, aggregates, false);
    }

    public VgiServiceImpl(Worker worker, List<ScalarFunction> scalars, List<TableFunction> tables,
                           List<TableInOutFunction> tableInOuts, List<AggregateFunction<?>> aggregates,
                           boolean sealOpaqueData) {
        this(worker, scalars, tables, tableInOuts, aggregates, sealOpaqueData, null);
    }

    /**
     * Variant for HTTP deployments that need a shared opaque-data key
     * across replicas. {@code opaqueDataKey} must be exactly 32 bytes when
     * set; when {@code null}, falls back to the per-process random key
     * (matches the single-replica contract).
     */
    public VgiServiceImpl(Worker worker, List<ScalarFunction> scalars, List<TableFunction> tables,
                           List<TableInOutFunction> tableInOuts, List<AggregateFunction<?>> aggregates,
                           boolean sealOpaqueData, byte[] opaqueDataKey) {
        this.sealer = (sealOpaqueData && opaqueDataKey != null)
                ? new OpaqueDataSealer(opaqueDataKey)
                : new OpaqueDataSealer(sealOpaqueData);
        this.worker = worker;
        for (ScalarFunction f : scalars) this.scalars.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        for (TableFunction f : tables) this.tables.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        for (TableInOutFunction f : tableInOuts) this.tableInOuts.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        for (AggregateFunction<?> f : aggregates) this.aggregates.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        for (var f : worker.bufferingFunctions())
            this.bufferingFns.computeIfAbsent(f.name(), k -> new ArrayList<>()).add(f);
        // Aggregate runner expects flat name → fn (no overloads in scope yet).
        Map<String, AggregateFunction<?>> aggFlat = new HashMap<>();
        for (var e : this.aggregates.entrySet()) aggFlat.put(e.getKey(), e.getValue().get(0));
        this.aggregateRunner = new AggregateRunner(aggFlat, storage);
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
            return bindTable(request, name, args, inputSchema, settings, argCount, token, ctx);
        }
        if (tableInOuts.containsKey(name)) {
            return bindTableInOut(request, name, args, inputSchema, settings, argCount, token);
        }
        if (bufferingFns.containsKey(name)) {
            return bindBuffering(request, name, args, inputSchema, settings, argCount, token, ctx);
        }
        throw new IllegalArgumentException("Unknown function: " + name);
    }

    private BindResponse bindBuffering(BindRequest request, String name, Arguments args,
                                        Schema inputSchema, Map<String, Object> settings,
                                        int argCount, byte[] token, CallContext ctx) {
        farm.query.vgi.buffering.TableBufferingFunction fn =
                OverloadResolver.pick(bufferingFns.get(name), argCount, args, inputSchema);
        AuthContext auth = authOf(ctx);
        byte[] attachPlain = sealer.unsealAttach(request.attach_opaque_data(), auth);
        BindResponse upstream = fn.onBind(new TableInOutBindParams(name, args, inputSchema, settings));
        Schema outputSchema = upstream.output_schema() == null
                ? null : SchemaUtil.deserializeSchema(upstream.output_schema());
        byte[] bindOpaque = upstream.opaque_data() == null ? new byte[0] : upstream.opaque_data();
        pendingBinds.put(bytesKey(token), new BoundBuffering(fn, args, inputSchema, outputSchema, settings,
                request.arguments(), request.settings(), upstream.output_schema(),
                attachPlain, bindOpaque));
        return new BindResponse(upstream.output_schema(), token,
                upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
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
                                    int argCount, byte[] token, CallContext ctx) {
        TableFunction fn = OverloadResolver.pick(tables.get(name), argCount, args, inputSchema);
        AuthContext auth = authOf(ctx);
        byte[] attachPlain = sealer.unsealAttach(request.attach_opaque_data(), auth);
        byte[] txnPlain = sealer.unsealTransaction(
                request.transaction_opaque_data(), request.attach_opaque_data(), auth);
        TableBindParams bindParams = new TableBindParams(name, args, inputSchema, settings,
                request.secrets(), request.resolved_secrets_provided(), attachPlain,
                transactionStore.view(txnPlain));
        BindResponse upstream = fn.onBind(bindParams);
        Schema outputSchema = upstream.output_schema() == null
                ? null : SchemaUtil.deserializeSchema(upstream.output_schema());
        byte[] bindOpaque = upstream.opaque_data() == null ? new byte[0] : upstream.opaque_data();
        pendingBinds.put(bytesKey(token), new BoundTable(fn, args, inputSchema, outputSchema, settings,
                request.arguments(), request.settings(), upstream.output_schema(),
                request.secrets(), attachPlain, bindOpaque));
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
        if (bound instanceof BoundBuffering bb) return initBuffering(request, bb, realOutputSchema, execId, header);
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
                bt.attachId(),
                bt.bindOpaqueData());
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
        // Narrow the output schema to the columns DuckDB requested when the
        // function opts into projection pushdown — mirrors initTable. The
        // narrowed schema is what both the exchange wire and the function's
        // params see, so the function emits only the requested columns.
        List<Integer> projIds = request.projection_ids() == null
                ? List.of() : request.projection_ids();
        Schema fnOutputSchema = realOutputSchema;
        if (bio.fn().metadata().projectionPushdown() && !projIds.isEmpty()) {
            fnOutputSchema = projectSchema(realOutputSchema, projIds);
        }
        TableInOutInitParams params = new TableInOutInitParams(
                bio.fn().name(), bio.args(), inputSchema, fnOutputSchema,
                bio.settings(), Allocators.root());
        TableInOutExchangeState state = bio.fn().createExchange(params);
        if (bio.fn().hasFinalize()) {
            tioExecutions.put(execKey, new TioExecutionState(bio.fn(), state, params));
        }
        return RpcStream.exchange(inputSchema, fnOutputSchema, state, header);
    }

    private RpcStream<? extends StreamState> initBuffering(InitRequest request, BoundBuffering bb,
                                                             Schema realOutputSchema, byte[] execId,
                                                             GlobalInitResponse header) {
        String phase = request.phase();
        if (!"TABLE_BUFFERING_FINALIZE".equalsIgnoreCase(phase)) {
            // Sink-side init (phase TABLE_BUFFERING): only mints execution_id.
            // No data stream — emit the header then immediate EOS.
            return RpcStream.producer(realOutputSchema,
                    new FinalizeProducerState(List.of()), header);
        }
        // Source-side init: one stream per finalize_state_id. Narrow the output
        // schema for projection pushdown (mirrors initTable) and build the
        // producer that drains buffered output for this finalize_state_id.
        List<Integer> projIds = request.projection_ids() == null
                ? List.of() : request.projection_ids();
        Schema fnOutputSchema = realOutputSchema;
        if (bb.fn().metadata().projectionPushdown() && !projIds.isEmpty()) {
            fnOutputSchema = projectSchema(realOutputSchema, projIds);
        }
        TableInitParams initParams = new TableInitParams(
                bb.fn().name(), bb.args(), fnOutputSchema, bb.settings(), Allocators.root(),
                request.pushdown_filters(), projIds,
                request.join_keys() == null ? List.of() : request.join_keys(),
                request.tablesample_percentage(), request.tablesample_seed(),
                request.order_by_column_name(), request.order_by_direction(),
                request.order_by_null_order(), request.order_by_limit(),
                execId, null, bb.attachId(), bb.bindOpaqueData());
        farm.query.vgi.buffering.BufferingStorage storage =
                new farm.query.vgi.buffering.BufferingStorage(this.storage, execId);
        farm.query.vgi.buffering.TableBufferingFinalizeParams fparams =
                new farm.query.vgi.buffering.TableBufferingFinalizeParams(
                        execId, request.finalize_state_id(), storage, initParams);
        TableProducerState producer = bb.fn().createFinalizeProducer(fparams);
        return RpcStream.producer(fnOutputSchema, producer, header);
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
    public byte[] table_function_statistics(byte[] request) {
        Map<String, byte[]> fields = IpcUnpacker.unpack(request, "bind_call", "bind_opaque_data");
        if (fields == null) return new byte[0];
        byte[] bindCall = fields.get("bind_call");
        byte[] bindOpaque = fields.get("bind_opaque_data");
        TableFunction fn = null;
        TableBindParams params = null;
        if (bindOpaque != null) {
            BoundEntry e = pendingBinds.get(bytesKey(bindOpaque));
            if (e instanceof BoundTable bt) {
                fn = bt.fn();
                params = new TableBindParams(fn.name(), bt.args(), bt.inputSchema(), bt.settings());
            }
        }
        if (fn == null && bindCall != null && bindCall.length > 0) {
            BindRequest embedded = RecordCodec.deserializeFromBytes(bindCall, BindRequest.class);
            if (tables.containsKey(embedded.function_name())) {
                Schema inputSchema = SchemaUtil.deserializeSchema(embedded.input_schema());
                Arguments args = ArgumentsParser.parse(embedded.arguments());
                Map<String, Object> settings = SettingsParser.parse(embedded.settings());
                int constN = args.positional().size();
                int colN = inputSchema == null ? 0 : inputSchema.getFields().size();
                fn = OverloadResolver.pick(tables.get(embedded.function_name()),
                        constN + colN, args, inputSchema);
                params = new TableBindParams(embedded.function_name(), args, inputSchema, settings);
            }
        }
        if (fn == null || params == null) return new byte[0];
        java.util.List<farm.query.vgi.catalog.ColumnStatistics> stats = fn.statistics(params);
        if (stats == null || stats.isEmpty()) return new byte[0];
        return ColumnStatisticsSerializer.serialize(stats);
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
                worker.dataVersionSpec(), attachOptionBytes,
                worker.releases(), worker.sourceUrl())));
    }

    @Override
    public farm.query.vgi.protocol.CatalogVersionResponse catalog_version(byte[] attach_opaque_data, byte[] transaction_opaque_data) {
        return new farm.query.vgi.protocol.CatalogVersionResponse(1L);
    }

    @Override
    public farm.query.vgi.protocol.TransactionBeginResponse catalog_transaction_begin(
            byte[] attach_opaque_data, CallContext ctx) {
        byte[] txnId = new byte[16];
        rng.nextBytes(txnId);
        transactionStore.begin(txnId);
        return new farm.query.vgi.protocol.TransactionBeginResponse(
                sealer.sealTransaction(txnId, attach_opaque_data, authOf(ctx)));
    }

    @Override
    public void catalog_transaction_commit(byte[] attach_opaque_data, byte[] transaction_opaque_data,
                                             CallContext ctx) {
        transactionStore.end(sealer.unsealTransaction(
                transaction_opaque_data, attach_opaque_data, authOf(ctx)));
    }

    @Override
    public void catalog_transaction_rollback(byte[] attach_opaque_data, byte[] transaction_opaque_data,
                                               CallContext ctx) {
        transactionStore.end(sealer.unsealTransaction(
                transaction_opaque_data, attach_opaque_data, authOf(ctx)));
    }

    private final CatalogRegistry catalogRegistry;

    @Override
    public CatalogAttachResult catalog_attach(CatalogAttachRequest request, CallContext ctx) {
        byte[] attachId;
        if (!worker.attachOptionSpecs().isEmpty()) {
            // attach_options pattern (Go/Python parity): encode merged
            // {defaults + user options} batch directly into attach_opaque_data so the
            // echo function is stateless under pool reuse / HTTP transport.
            // attach_opaque_data = uuid(16) || 0x00 || ipc(mergedBatch).
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
                sealer.sealAttach(attachId, authOf(ctx)),
                true, true, false,  // supports_transactions, supports_time_travel, catalog_version_frozen
                1L,
                false,
                worker.defaultSchema(),
                settings,
                secretTypes,
                worker.catalogComment(),
                tags,
                true,
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
    public void catalog_detach(byte[] attach_opaque_data) {
    }

    @Override
    public ItemsResponse catalog_schemas(byte[] attach_opaque_data, byte[] transaction_opaque_data) {
        List<byte[]> items = new ArrayList<>();
        for (SchemaDesc s : workerSchemas()) {
            items.add(RecordCodec.serializeToBytes(
                    new SchemaInfo(s.comment, Map.of(), attach_opaque_data, s.name, schemaCounts(s))));
        }
        return new ItemsResponse(items);
    }

    @Override
    public ItemsResponse catalog_schema_get(byte[] attach_opaque_data, String name, byte[] transaction_opaque_data) {
        for (SchemaDesc s : workerSchemas()) {
            if (s.name.equals(name)) {
                return new ItemsResponse(List.of(RecordCodec.serializeToBytes(
                        new SchemaInfo(s.comment, Map.of(), attach_opaque_data, name, schemaCounts(s)))));
            }
        }
        return ItemsResponse.empty();
    }

    /** Schema descriptors registered with the worker (default + auxiliary). */
    private record SchemaDesc(String name, String comment) {}

    /**
     * Schemas advertised to DuckDB. Always includes {@link
     * Worker#defaultSchema()}; auxiliary schemas are added only if at least
     * one registered catalog table, view, or macro lives in them. This avoids
     * surfacing phantom empty schemas for workers that don't use them
     * (e.g. our example worker uses "data" for its catalog tables; a custom
     * worker with only "main" tables shouldn't expose "data" at all).
     */
    private List<SchemaDesc> workerSchemas() {
        List<SchemaDesc> result = new ArrayList<>();
        Map<String, String> comments = worker.schemaComments();
        String defaultSchema = worker.defaultSchema();
        result.add(new SchemaDesc(defaultSchema,
                comments.getOrDefault(defaultSchema, "Default schema")));
        java.util.LinkedHashSet<String> extras = new java.util.LinkedHashSet<>();
        for (var t : worker.catalogTables()) {
            if (!defaultSchema.equals(t.schema())) extras.add(t.schema());
        }
        for (var v : worker.views()) {
            if (!defaultSchema.equals(v.schema())) extras.add(v.schema());
        }
        for (var m : worker.macros()) {
            if (!defaultSchema.equals(m.schema())) extras.add(m.schema());
        }
        for (String s : extras) result.add(new SchemaDesc(s, comments.getOrDefault(s, "")));
        return result;
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
            byte[] attach_opaque_data, String name, byte[] transaction_opaque_data, CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        List<byte[]> items = new ArrayList<>();
        String dv = catalogRegistry.dataVersion(attach_opaque_data_plain);
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
            if (catalogRegistry.isHiddenInVersionedTables(t.name(), attach_opaque_data_plain)) continue;
            CatalogTable resolved = dv == null ? t : catalogRegistry.resolveVersion(t, "data_version", dv);
            items.add(TableInfoSerializer.serialize(toTableInfo(resolved)));
        }
        return new ItemsResponse(items);
    }

    @Override
    public farm.query.vgi.protocol.TableScanFunctionGetResponse catalog_table_scan_function_get(
            byte[] attach_opaque_data, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_opaque_data, CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        var at = catalogRegistry.effectiveAt(attach_opaque_data_plain, at_unit, at_value);
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
    public byte[] catalog_table_scan_branches_get(
            byte[] attach_opaque_data, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_opaque_data, CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        // Explicitly declared multi-branch table — return its branches as-is
        // (an empty list deliberately reaches the C++ loud-fail path).
        List<farm.query.vgi.catalog.ScanBranch> branches =
                worker.multiBranchTable(schema_name, name);
        if (branches != null) {
            return ScanBranchesResultSerializer.serialize(branches, List.of());
        }
        // Every other scannable table wraps its single scan function as one
        // branch. The C++ capability cache is per-attach, not per-table, so we
        // can't selectively raise method-not-implemented here.
        var at = catalogRegistry.effectiveAt(attach_opaque_data_plain, at_unit, at_value);
        for (CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                CatalogTable resolved = catalogRegistry.resolveVersion(t, at.unit(), at.value());
                if (resolved.scanFunctionName() == null) break;
                farm.query.vgi.catalog.ScanBranch one = new farm.query.vgi.catalog.ScanBranch(
                        resolved.scanFunctionName(),
                        resolved.scanFunctionPositional() == null ? List.of() : resolved.scanFunctionPositional(),
                        resolved.scanFunctionNamed() == null ? Map.of() : resolved.scanFunctionNamed(),
                        null, false);
                return ScanBranchesResultSerializer.serialize(List.of(one), List.of());
            }
        }
        throw new IllegalArgumentException(
                "scan_branches_get: unknown table " + schema_name + "." + name);
    }

    // -----------------------------------------------------------------------
    // Table buffering (Sink+Source) lifecycle
    // -----------------------------------------------------------------------

    private farm.query.vgi.buffering.TableBufferingFunction bufferingFn(String name) {
        var list = bufferingFns.get(name);
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("Unknown buffering function: " + name);
        }
        return list.get(0);
    }

    @Override
    public farm.query.vgi.protocol.TableBufferingProcessResponse table_buffering_process(
            farm.query.vgi.protocol.TableBufferingProcessRequest request, CallContext ctx) {
        var fn = bufferingFn(request.function_name());
        farm.query.vgi.buffering.BufferingStorage storage =
                new farm.query.vgi.buffering.BufferingStorage(this.storage, request.execution_id());
        var params = new farm.query.vgi.buffering.TableBufferingProcessParams(
                request.function_name(), request.execution_id(), storage, request.batch_index(), ctx);
        byte[] stateId;
        try (org.apache.arrow.vector.VectorSchemaRoot root =
                     BatchUtil.readSingleBatch(request.input_batch(), Allocators.root())) {
            stateId = fn.process(root, params);
        }
        return new farm.query.vgi.protocol.TableBufferingProcessResponse(stateId);
    }

    @Override
    public farm.query.vgi.protocol.TableBufferingCombineResponse table_buffering_combine(
            farm.query.vgi.protocol.TableBufferingCombineRequest request, CallContext ctx) {
        var fn = bufferingFn(request.function_name());
        farm.query.vgi.buffering.BufferingStorage storage =
                new farm.query.vgi.buffering.BufferingStorage(this.storage, request.execution_id());
        var params = new farm.query.vgi.buffering.TableBufferingCombineParams(
                request.function_name(), request.execution_id(), storage, ctx);
        List<byte[]> ids = fn.combine(
                request.state_ids() == null ? List.of() : request.state_ids(), params);
        return new farm.query.vgi.protocol.TableBufferingCombineResponse(ids);
    }

    @Override
    public farm.query.vgi.protocol.TableBufferingDestructorResponse table_buffering_destructor(
            farm.query.vgi.protocol.TableBufferingDestructorRequest request) {
        this.storage.executionClear(request.execution_id());
        return new farm.query.vgi.protocol.TableBufferingDestructorResponse();
    }

    @Override
    public ItemsResponse catalog_table_get(
            byte[] attach_opaque_data, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_opaque_data, CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        if (catalogRegistry.isHiddenInVersionedTables(name, attach_opaque_data_plain)) return ItemsResponse.empty();
        var at = catalogRegistry.effectiveAt(attach_opaque_data_plain, at_unit, at_value);
        boolean isMultiBranch = worker.multiBranchTable(schema_name, name) != null;
        for (CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                // Multi-branch tables don't honour AT — the C++ extension
                // refuses it loudly at scan time. Pass the base table through
                // here so binding reaches that refusal instead of failing with
                // a generic time-travel error from resolveVersion.
                CatalogTable resolved = isMultiBranch
                        ? t : catalogRegistry.resolveVersion(t, at.unit(), at.value());
                return new ItemsResponse(List.of(TableInfoSerializer.serialize(toTableInfo(resolved))));
            }
        }
        return ItemsResponse.empty();
    }

    @Override
    public byte[] catalog_table_column_statistics_get(
            byte[] attach_opaque_data, String schema_name, String name, byte[] transaction_opaque_data,
            CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        if (catalogRegistry.isHiddenInVersionedTables(name, attach_opaque_data_plain)) return new byte[0];
        for (CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                if (t.statistics() == null || t.statistics().isEmpty()) return new byte[0];
                return ColumnStatisticsSerializer.serialize(t.statistics());
            }
        }
        return new byte[0];
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
        boolean hasStats = t.statistics() != null && !t.statistics().isEmpty();
        byte[] inlineStats = hasStats
                ? ColumnStatisticsSerializer.serialize(t.statistics()) : null;
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
                hasStats,
                scanFn,
                null,
                null,
                null,
                t.inlineCardinality() ? t.cardinalityEstimate() : null,
                t.inlineCardinality() ? t.cardinalityMax() : null,
                inlineStats,
                null);
    }

    @Override
    public ItemsResponse catalog_schema_contents_views(
            byte[] attach_opaque_data, String name, byte[] transaction_opaque_data) {
        List<byte[]> items = new ArrayList<>();
        // Versioned-tables catalog ships no user-visible views.
        if ("versioned_tables".equals(worker.catalogName())) {
            return new ItemsResponse(items);
        }
        for (View v : worker.views()) {
            if (!v.schema().equals(name)) continue;
            items.add(RecordCodec.serializeToBytes(new farm.query.vgi.protocol.ViewInfo(
                    v.comment(), v.tags(), v.name(), v.schema(), v.definition(), v.columnComments())));
        }
        return new ItemsResponse(items);
    }

    @Override
    public ItemsResponse catalog_view_get(byte[] attach_opaque_data, String schema_name, String name, byte[] transaction_opaque_data) {
        for (View v : worker.views()) {
            if (v.schema().equals(schema_name) && v.name().equals(name)) {
                return new ItemsResponse(List.of(RecordCodec.serializeToBytes(
                        new farm.query.vgi.protocol.ViewInfo(v.comment(), v.tags(),
                                v.name(), v.schema(), v.definition(), v.columnComments()))));
            }
        }
        return ItemsResponse.empty();
    }

    @Override
    public ItemsResponse catalog_schema_contents_macros(
            byte[] attach_opaque_data, String name, String type, byte[] transaction_opaque_data) {
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
    public ItemsResponse catalog_macro_get(byte[] attach_opaque_data, String schema_name, String name, byte[] transaction_opaque_data) {
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
            byte[] attach_opaque_data, String name, String type, byte[] transaction_opaque_data,
            CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
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
            String attachCatName = catalogRegistry.catalogName(attach_opaque_data_plain);
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
            // Buffering (Sink+Source) functions also surface as table functions;
            // their FunctionInfo.function_type="table_buffering" selects the
            // C++ buffering operator.
            for (var variants : bufferingFns.values()) {
                for (var fn : variants) {
                    items.add(FunctionInfoSerializer.serialize(toBufferingFunctionInfo(fn, name)));
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

    private FunctionInfo toBufferingFunctionInfo(
            farm.query.vgi.buffering.TableBufferingFunction fn, String schemaName) {
        BindResponse r = fn.onBind(new TableInOutBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        FunctionInfo base = baseFunctionInfo(fn.name(), fn.metadata(), schemaName, "table_buffering",
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()), bindOutput(r),
                /*hasFinalize=*/true, 1);
        // Re-stamp the buffering-specific ordering flags (baseFunctionInfo
        // hardcodes them false; they're only meaningful for TableBuffering).
        return new FunctionInfo(
                base.comment(), base.tags(), base.name(), base.schema_name(), base.function_type(),
                base.arguments(), base.output_schema(), base.stability(), base.null_handling(),
                base.description(), base.examples(), base.categories(), base.projection_pushdown(),
                base.filter_pushdown(), base.sampling_pushdown(), base.late_materialization(),
                base.supported_expression_filters(),
                base.order_preservation(), base.max_workers(), base.supports_batch_index(),
                base.partition_kind(), base.order_dependent(), base.distinct_dependent(),
                base.supports_window(), base.streaming_partitioned(), base.has_finalize(),
                fn.sourceOrderDependent(), fn.sinkOrderDependent(), fn.requiresInputBatchIndex(),
                base.required_settings(), base.required_secrets());
    }

    private static byte[] bindOutput(BindResponse r) {
        return r.output_schema() != null ? r.output_schema() : new byte[0];
    }

    private FunctionInfo baseFunctionInfo(farm.query.vgi.function.FunctionDescriptor fn,
                                           String schemaName, String type,
                                           byte[] outputSchema, boolean hasFinalize) {
        // Only TableFunction has a meaningful maxWorkers; everything else
        // is single-worker by definition.
        int maxWorkers = fn instanceof TableFunction tf ? (int) tf.maxWorkers() : 1;
        return baseFunctionInfo(fn.name(), fn.metadata(), schemaName, type,
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()), outputSchema, hasFinalize,
                maxWorkers);
    }

    private FunctionInfo baseFunctionInfo(String name, FunctionMetadata md, String schemaName,
                                           String type, byte[] arguments, byte[] outputSchema,
                                           boolean hasFinalize, int maxWorkers) {
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
                md.lateMaterialization() ? Boolean.TRUE : null,
                List.of(),
                md.orderPreservation() == null ? null : md.orderPreservation().name(),
                maxWorkers,
                md.supportsBatchIndex(),
                md.partitionKind() == null ? "NOT_PARTITIONED" : md.partitionKind().name(),
                "NOT_ORDER_DEPENDENT",
                "NOT_DISTINCT_DEPENDENT",
                false,
                false,  // streaming_partitioned
                hasFinalize,
                false,  // source_order_dependent — only meaningful for TableBuffering
                false,  // sink_order_dependent
                false,  // requires_input_batch_index
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
