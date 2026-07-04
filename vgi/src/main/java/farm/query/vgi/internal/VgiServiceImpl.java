// Copyright 2026 Query Farm LLC - https://query.farm

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

/**
 * The default {@link VgiService} implementation that dispatches every VGI RPC
 * to the functions, catalog tables, views and macros registered on a
 * {@link Worker}.
 *
 * <p>Holds the bind cache ({@code bind} → {@code init} hand-off keyed by an
 * opaque token), the shared-state backend, the transaction store, the
 * aggregate runner and the read-only catalog registry. Function execution
 * (scalar / table / table-in-out / buffering) flows through {@link #bind} and
 * {@link #init}; the remaining methods serve the catalog-introspection and
 * statistics RPCs. Construction also installs the process-wide
 * {@link ServiceLocator}.</p>
 */
public final class VgiServiceImpl implements VgiService {

    /**
     * Test-fixture hook: when enabled, the {@code double} scalar advertises an
     * unrecognized {@code null_handling} value ("WEIRD") to drive the C++
     * parser's strict-enum rejection ({@code bad_enum.test}). Enabled only by
     * the dedicated bad-enum worker process via {@link #enableBadEnum()}.
     */
    static volatile boolean BAD_ENUM_MODE = false;

    /** Turn on the {@code bad_enum} fixture behaviour (see {@link #BAD_ENUM_MODE}). */
    public static void enableBadEnum() {
        BAD_ENUM_MODE = true;
        FunctionInfoSerializer.enableBadEnumNullHandling();
    }

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
    // Expose the backend process-wide so a buffering source producer can re-bind
    // its storage after an HTTP state-token round-trip (see BufferingStorageHolder).
    { farm.query.vgi.buffering.BufferingStorageHolder.register(storage); }
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
    private final java.util.Map<String, BoundEntry> pendingBinds = BoundedMap.create(MAX_PENDING_BINDS);
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
                               byte[] secrets, byte[] attachId, byte[] bindOpaqueData,
                               String atUnit, String atValue,
                               farm.query.vgi.protocol.CopyFromContext copyFrom)
            implements BoundEntry {}

    private record BoundTableInOut(TableInOutFunction fn, Arguments args, Schema inputSchema,
                                    Schema outputSchema, Map<String, Object> settings,
                                    byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc,
                                    byte[] secrets, byte[] attachId)
            implements BoundEntry {}

    private record BoundBuffering(farm.query.vgi.buffering.TableBufferingFunction fn, Arguments args,
                                   Schema inputSchema, Schema outputSchema, Map<String, Object> settings,
                                   byte[] argumentsIpc, byte[] settingsIpc, byte[] outputSchemaIpc,
                                   byte[] secrets, byte[] attachId, byte[] bindOpaqueData,
                                   byte[] inputSchemaIpc,
                                   farm.query.vgi.protocol.CopyToContext copyTo)
            implements BoundEntry {}

    /**
     * Construct a service with opaque-data sealing disabled (stdio / AF_UNIX).
     *
     * @param worker the owning worker (catalog metadata, buffering functions, settings, secrets)
     * @param scalars registered scalar functions
     * @param tables registered table functions
     * @param tableInOuts registered table-in-out functions
     * @param aggregates registered aggregate functions
     */
    public VgiServiceImpl(Worker worker, List<ScalarFunction> scalars, List<TableFunction> tables,
                           List<TableInOutFunction> tableInOuts, List<AggregateFunction<?>> aggregates) {
        this(worker, scalars, tables, tableInOuts, aggregates, false);
    }

    /**
     * Construct a service, optionally enabling per-process opaque-data sealing (HTTP transport).
     *
     * @param worker the owning worker
     * @param scalars registered scalar functions
     * @param tables registered table functions
     * @param tableInOuts registered table-in-out functions
     * @param aggregates registered aggregate functions
     * @param sealOpaqueData {@code true} to AEAD-seal attach / transaction opaque data with a per-process key
     */
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
     *
     * @param worker the owning worker
     * @param scalars registered scalar functions
     * @param tables registered table functions
     * @param tableInOuts registered table-in-out functions
     * @param aggregates registered aggregate functions
     * @param sealOpaqueData {@code true} to AEAD-seal attach / transaction opaque data
     * @param opaqueDataKey exactly 32 bytes of shared key material for cross-replica sealing, or {@code null} for a per-process key
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

    /**
     * Bind a function call: resolve the overload, run its {@code onBind}, and
     * cache the result under a fresh opaque token for the matching {@link #init}.
     *
     * @param request the bind request (function name, input schema, arguments, settings, opaque data)
     * @param ctx the per-call RPC context (auth principal for opaque-data unsealing)
     * @return the bind response carrying the opaque token and output schema
     * @throws IllegalArgumentException if no function of that name is registered
     */
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
            return bindTableInOut(request, name, args, inputSchema, settings, argCount, token, ctx);
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
        BindResponse upstream = fn.onBind(new TableInOutBindParams(name, args, inputSchema, settings,
                request.secrets(), request.resolved_secrets_provided(),
                attachPlain, attachScopedStorage(attachPlain), request.copy_to()));
        Schema outputSchema = upstream.output_schema() == null
                ? null : SchemaUtil.deserializeSchema(upstream.output_schema());
        byte[] bindOpaque = upstream.opaque_data() == null ? new byte[0] : upstream.opaque_data();
        pendingBinds.put(bytesKey(token), new BoundBuffering(fn, args, inputSchema, outputSchema, settings,
                request.arguments(), request.settings(), upstream.output_schema(),
                request.secrets(), attachPlain, bindOpaque,
                request.input_schema(), request.copy_to()));
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
                transactionStore.view(txnPlain, attachPlain), attachScopedStorage(attachPlain),
                request.copy_from());
        BindResponse upstream = fn.onBind(bindParams);
        Schema outputSchema = upstream.output_schema() == null
                ? null : SchemaUtil.deserializeSchema(upstream.output_schema());
        byte[] bindOpaque = upstream.opaque_data() == null ? new byte[0] : upstream.opaque_data();
        pendingBinds.put(bytesKey(token), new BoundTable(fn, args, inputSchema, outputSchema, settings,
                request.arguments(), request.settings(), upstream.output_schema(),
                request.secrets(), attachPlain, bindOpaque, request.at_unit(), request.at_value(),
                request.copy_from()));
        return new BindResponse(upstream.output_schema(), token,
                upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
    }

    private BindResponse bindTableInOut(BindRequest request, String name, Arguments args,
                                         Schema inputSchema, Map<String, Object> settings,
                                         int argCount, byte[] token, CallContext ctx) {
        TableInOutFunction fn = OverloadResolver.pick(tableInOuts.get(name), argCount, args, inputSchema);
        byte[] attachPlain = sealer.unsealAttach(request.attach_opaque_data(), authOf(ctx));
        BindResponse upstream = fn.onBind(new TableInOutBindParams(name, args, inputSchema, settings,
                request.secrets(), request.resolved_secrets_provided(),
                attachPlain, attachScopedStorage(attachPlain)));
        Schema outputSchema = upstream.output_schema() == null
                ? null : SchemaUtil.deserializeSchema(upstream.output_schema());
        pendingBinds.put(bytesKey(token), new BoundTableInOut(fn, args, inputSchema, outputSchema, settings,
                request.arguments(), request.settings(), upstream.output_schema(),
                request.secrets(), attachPlain));
        return new BindResponse(upstream.output_schema(), token,
                upstream.lookup_secret_types(), upstream.lookup_scopes(), upstream.lookup_names());
    }

    /**
     * Initialise a bound function into an executable RPC stream. Picks up the
     * cached bind by {@code bind_opaque_data}, or runs an inlined {@code bind_call}
     * to save the round-trip.
     *
     * @param request the init request (bind reference, real output schema, pushdown / ordering hints, phase)
     * @param ctx the per-call RPC context
     * @return a producer or exchange stream over the function's output
     * @throws IllegalStateException if no bind context can be located
     */
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
                bt.bindOpaqueData(),
                bt.atUnit(),
                bt.atValue(),
                new farm.query.vgi.storage.BoundStorage(this.storage, execId, bt.attachId()),
                bt.copyFrom());
        TableProducerState state = bt.fn().createProducer(params);
        return RpcStream.producer(fnOutputSchema, state, header);
    }

    private RpcStream<? extends StreamState> initTableInOut(InitRequest request, BoundTableInOut bio,
                                                              Schema realOutputSchema, byte[] execId,
                                                              GlobalInitResponse header) {
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
                bio.settings(), Allocators.root(),
                new farm.query.vgi.storage.BoundStorage(this.storage, execId, bio.attachId()),
                bio.secrets());
        TableInOutExchangeState state = bio.fn().createExchange(params);
        return RpcStream.exchange(inputSchema, fnOutputSchema, state, header);
    }

    private RpcStream<? extends StreamState> initBuffering(InitRequest request, BoundBuffering bb,
                                                             Schema realOutputSchema, byte[] execId,
                                                             GlobalInitResponse header) {
        String phase = request.phase();
        if (!"TABLE_BUFFERING_FINALIZE".equalsIgnoreCase(phase)) {
            // Sink-side init (phase TABLE_BUFFERING): mints execution_id and
            // persists the init metadata so any pool worker can cold-load it
            // to serve the subsequent process/combine RPCs (Python parity).
            new farm.query.vgi.storage.BoundStorage(this.storage, execId, bb.attachId())
                    .statePut(farm.query.vgi.storage.FrameworkNs.BUFFERING_INIT,
                            farm.query.vgi.storage.BoundStorage.packIntKey(-1),
                            RecordCodec.serializeToBytes(new BufferingInitState(
                                    bb.argumentsIpc(), bb.settingsIpc(),
                                    bb.outputSchemaIpc(), bb.attachId(),
                                    bb.inputSchemaIpc(), bb.copyTo(), bb.secrets())));
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
        farm.query.vgi.storage.BoundStorage storage =
                new farm.query.vgi.storage.BoundStorage(this.storage, execId, bb.attachId());
        TableInitParams initParams = new TableInitParams(
                bb.fn().name(), bb.args(), fnOutputSchema, bb.settings(), Allocators.root(),
                request.pushdown_filters(), projIds,
                request.join_keys() == null ? List.of() : request.join_keys(),
                request.tablesample_percentage(), request.tablesample_seed(),
                request.order_by_column_name(), request.order_by_direction(),
                request.order_by_null_order(), request.order_by_limit(),
                execId, bb.secrets(), bb.attachId(), bb.bindOpaqueData(), null, null, storage, null);
        farm.query.vgi.buffering.TableBufferingFinalizeParams fparams =
                new farm.query.vgi.buffering.TableBufferingFinalizeParams(
                        execId, request.finalize_state_id(), bb.attachId(), storage, initParams);
        TableProducerState producer = bb.fn().createFinalizeProducer(fparams);
        return RpcStream.producer(fnOutputSchema, producer, header);
    }

    /**
     * Resolve an AT clause to one of the {@code tt_pushdown_cols} fixture's data
     * versions (1 or 2): no AT -> current version (2); {@code VERSION => n} ->
     * {@code n}; {@code TIMESTAMP} -> year &le; 2020 maps to 1, else 2. Mirrors
     * the vgi-python {@code resolve_tt_version} fixture helper.
     */
    private static int resolveTtVersion(String atUnit, String atValue) {
        if (atUnit == null || atUnit.isEmpty()) return 2;
        if ("version".equalsIgnoreCase(atUnit)) {
            try { return Integer.parseInt(atValue); }
            catch (NumberFormatException e) { throw new IllegalArgumentException("Unknown version: " + atValue); }
        }
        if ("timestamp".equalsIgnoreCase(atUnit)) {
            int year;
            try { year = Integer.parseInt(atValue.substring(0, Math.min(4, atValue.length()))); }
            catch (RuntimeException e) { throw new IllegalArgumentException("Unknown timestamp: " + atValue); }
            return year <= 2020 ? 1 : 2;
        }
        throw new IllegalArgumentException("Unsupported at_unit: " + atUnit);
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

    /**
     * Estimate a table function's row cardinality.
     *
     * @param request a 1-row IPC struct carrying {@code bind_call} and {@code bind_opaque_data}
     * @return the estimate / max (both {@code null} when the function gives no estimate)
     */
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

    /**
     * Per-column statistics for a function-only table binding (e.g. {@code example.sequence(N)}).
     *
     * @param request a 1-row IPC struct carrying {@code bind_call} and {@code bind_opaque_data}
     * @return serialised {@code ColumnStatistics}, or empty bytes when the function provides none
     */
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

    /**
     * Diagnostic key/value pairs DuckDB renders in {@code EXPLAIN} for a bound table function.
     *
     * @param request a 1-row IPC struct carrying {@code bind_call}, {@code bind_opaque_data} and {@code global_execution_id}
     * @return parallel key / value lists, both empty when the function has nothing to report
     */
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

    /**
     * Bind an aggregate function (resolves its output schema for the planner).
     *
     * @param request the aggregate bind request
     * @return the bind response carrying the output schema
     */
    @Override
    public AggregateBindResponse aggregate_bind(AggregateBindRequest request) {
        return aggregateRunner.bind(request.function_name(), request.input_schema(), request.arguments(),
                request.secrets());
    }

    /**
     * Feed one input batch into the per-group aggregate state.
     *
     * @param request the update request (function, execution id, input batch)
     * @return an empty acknowledgement
     */
    @Override
    public farm.query.vgi.protocol.AggregateUpdateResponse aggregate_update(AggregateUpdateRequest request) {
        aggregateRunner.update(request.function_name(), request.execution_id(), request.input_batch());
        return new farm.query.vgi.protocol.AggregateUpdateResponse();
    }

    /**
     * Merge one partial aggregate state into another (parallel aggregation).
     *
     * @param request the combine request (function, execution id, merge batch)
     * @return an empty acknowledgement
     */
    @Override
    public farm.query.vgi.protocol.AggregateCombineResponse aggregate_combine(AggregateCombineRequest request) {
        aggregateRunner.combine(request.function_name(), request.execution_id(), request.merge_batch());
        return new farm.query.vgi.protocol.AggregateCombineResponse();
    }

    /**
     * Produce final aggregate values for the requested group ids.
     *
     * @param request the finalize request (function, execution id, group ids, output schema)
     * @return the finalize response batch
     */
    @Override
    public AggregateFinalizeResponse aggregate_finalize(AggregateFinalizeRequest request) {
        return aggregateRunner.finalizeRequest(
                request.function_name(), request.execution_id(),
                request.group_ids_batch(), request.output_schema());
    }

    /**
     * Release per-group aggregate state once a group is fully consumed.
     *
     * @param request the destructor request (function, execution id, group ids)
     * @return an empty acknowledgement
     */
    @Override
    public farm.query.vgi.protocol.AggregateDestructorResponse aggregate_destructor(AggregateDestructorRequest request) {
        aggregateRunner.destructor(
                request.function_name(), request.execution_id(), request.group_ids_batch());
        return new farm.query.vgi.protocol.AggregateDestructorResponse();
    }

    // -----------------------------------------------------------------------
    // Read-only catalog
    // -----------------------------------------------------------------------

    /**
     * List the single catalog this worker serves, with its version manifest and attach options.
     *
     * @return a one-item response holding the serialised {@code CatalogInfo}
     */
    @Override
    public ItemsResponse catalog_catalogs() {
        List<byte[]> attachOptionBytes = new ArrayList<>();
        for (farm.query.vgi.AttachOptionSpec spec : worker.attachOptionSpecs()) {
            attachOptionBytes.add(AttachOptionSpecSerializer.serialize(spec));
        }
        List<byte[]> items = new ArrayList<>();
        items.add(CatalogInfoSerializer.serialize(
                worker.catalogName(), worker.implementationVersion(),
                worker.dataVersionSpec(), attachOptionBytes,
                worker.releases(), worker.sourceUrl()));
        for (Worker.ExtraCatalog extra : worker.extraCatalogs().values()) {
            items.add(CatalogInfoSerializer.serialize(
                    extra.name(), extra.implementationVersion(), extra.dataVersion(),
                    List.of(), List.of(), null));
        }
        return new ItemsResponse(items);
    }

    /**
     * The catalog's monotonically-increasing version counter (always 1 — this catalog is immutable).
     *
     * @param attach_opaque_data the attach token
     * @param transaction_opaque_data the optional transaction token
     * @return the catalog version response
     */
    @Override
    public farm.query.vgi.protocol.CatalogVersionResponse catalog_version(byte[] attach_opaque_data,
            byte[] transaction_opaque_data, CallContext ctx) {
        // Versioned fixture: assert the routing cookie set at ATTACH is echoed
        // back. Over HTTP a populated cookie jar that's missing vgi_sticky means
        // the client failed to plumb Set-Cookie -> Cookie. Over subprocess the
        // cookie map is empty, so the check is skipped. (vgi-python parity.)
        if ("versioned".equals(worker.catalogName()) && ctx != null
                && !ctx.cookies().isEmpty() && !ctx.cookies().containsKey("vgi_sticky")) {
            throw new IllegalStateException(
                    "expected cookie 'vgi_sticky' on follow-up request; got " + ctx.cookies().keySet());
        }
        return new farm.query.vgi.protocol.CatalogVersionResponse(1L);
    }

    /**
     * Begin a transaction: mint a fresh id, mark it active, and return its sealed token.
     *
     * @param attach_opaque_data the attach token (sealed into the transaction AAD)
     * @param ctx the per-call RPC context
     * @return the begin response carrying the (sealed) {@code transaction_opaque_data}
     */
    @Override
    public farm.query.vgi.protocol.TransactionBeginResponse catalog_transaction_begin(
            byte[] attach_opaque_data, CallContext ctx) {
        byte[] txnId = new byte[16];
        rng.nextBytes(txnId);
        AuthContext auth = authOf(ctx);
        transactionStore.begin(txnId, sealer.unsealAttach(attach_opaque_data, auth));
        return new farm.query.vgi.protocol.TransactionBeginResponse(
                sealer.sealTransaction(txnId, attach_opaque_data, auth));
    }

    /**
     * Commit a transaction (clears its marker and cached state).
     *
     * @param attach_opaque_data the attach token
     * @param transaction_opaque_data the transaction token to commit
     * @param ctx the per-call RPC context
     */
    @Override
    public void catalog_transaction_commit(byte[] attach_opaque_data, byte[] transaction_opaque_data,
                                             CallContext ctx) {
        AuthContext auth = authOf(ctx);
        transactionStore.end(
                sealer.unsealTransaction(transaction_opaque_data, attach_opaque_data, auth),
                sealer.unsealAttach(attach_opaque_data, auth));
    }

    /**
     * Roll back a transaction (clears its marker and cached state — same effect as commit here).
     *
     * @param attach_opaque_data the attach token
     * @param transaction_opaque_data the transaction token to roll back
     * @param ctx the per-call RPC context
     */
    @Override
    public void catalog_transaction_rollback(byte[] attach_opaque_data, byte[] transaction_opaque_data,
                                               CallContext ctx) {
        AuthContext auth = authOf(ctx);
        transactionStore.end(
                sealer.unsealTransaction(transaction_opaque_data, attach_opaque_data, auth),
                sealer.unsealAttach(attach_opaque_data, auth));
    }

    private final CatalogRegistry catalogRegistry;

    /**
     * Attach the catalog: mint the attach token, resolve the requested
     * implementation / data versions, and advertise capabilities, settings and
     * secret types.
     *
     * @param request the attach request (name, attach options, requested version specs)
     * @param ctx the per-call RPC context
     * @return the attach result with the (sealed) attach token and capability flags
     * @throws IllegalArgumentException if a requested version spec can't be satisfied
     */
    @Override
    public CatalogAttachResult catalog_attach(CatalogAttachRequest request, CallContext ctx) {
        Worker.ExtraCatalog extra = worker.extraCatalogs().get(request.name());
        if (extra != null) {
            // MetaWorker-style auxiliary catalog: a random per-ATTACH opaque id
            // is the storage scope isolating this session's state; the client
            // persists and resends it, so it also survives worker restarts.
            byte[] extraAttachId = new byte[16];
            rng.nextBytes(extraAttachId);
            catalogRegistry.recordAttach(extraAttachId, extra.dataVersion(), request.name());
            return new CatalogAttachResult(
                    sealer.sealAttach(extraAttachId, authOf(ctx)),
                    false, false, false,
                    1L,
                    true,  // attach_opaque_data_required
                    "main",
                    List.of(), List.of(), List.of(),
                    extra.schemaComment(),
                    Map.of(),
                    false,
                    extra.dataVersion(),
                    extra.implementationVersion());
        }
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
        List<byte[]> attachCatalogs = new ArrayList<>();
        for (farm.query.vgi.protocol.AttachCatalogInfo info : worker.attachCatalogInfos()) {
            attachCatalogs.add(farm.query.vgirpc.marshal.RecordCodec.serializeToBytes(info));
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
        // Versioned fixture: pin an HTTP routing cookie so the C++ cookie jar
        // round-trips Set-Cookie -> Cookie; catalog_version asserts it on every
        // follow-up RPC (attach/versioning_http.test). No-op on non-HTTP
        // transports (set_cookie has no sink there), matching vgi-python's
        // VersionedCatalog.
        if ("versioned".equals(worker.catalogName()) && ctx != null) {
            ctx.setCookie("vgi_sticky", java.util.UUID.randomUUID().toString().replace("-", ""));
        }
        return new CatalogAttachResult(
                sealer.sealAttach(attachId, authOf(ctx)),
                true, true, false,  // supports_transactions, supports_time_travel, catalog_version_frozen
                1L,
                false,
                worker.defaultSchema(),
                settings,
                secretTypes,
                attachCatalogs,
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


    /**
     * Detach the catalog. No-op: attach state is encoded into the token, not held server-side.
     *
     * @param attach_opaque_data the attach token being released
     */
    @Override
    public void catalog_detach(byte[] attach_opaque_data) {
    }

    /**
     * List the schemas this worker exposes (default plus any non-empty auxiliary schemas).
     *
     * @param attach_opaque_data the attach token
     * @param transaction_opaque_data the optional transaction token
     * @return one serialised {@code SchemaInfo} per schema
     */
    @Override
    public ItemsResponse catalog_schemas(byte[] attach_opaque_data, byte[] transaction_opaque_data) {
        Worker.ExtraCatalog extra = extraCatalogOf(attach_opaque_data);
        if (extra != null) {
            return new ItemsResponse(List.of(RecordCodec.serializeToBytes(
                    new SchemaInfo(extra.schemaComment(), Map.of(), attach_opaque_data, "main",
                            extraSchemaCounts(extra)))));
        }
        List<byte[]> items = new ArrayList<>();
        for (SchemaDesc s : workerSchemas()) {
            items.add(RecordCodec.serializeToBytes(
                    new SchemaInfo(s.comment, s.tags, attach_opaque_data, s.name, schemaCounts(s))));
        }
        return new ItemsResponse(items);
    }

    /**
     * Fetch a single schema by name.
     *
     * @param attach_opaque_data the attach token
     * @param name the schema name
     * @param transaction_opaque_data the optional transaction token
     * @return a one-item response, or empty when the schema is unknown
     */
    @Override
    public ItemsResponse catalog_schema_get(byte[] attach_opaque_data, String name, byte[] transaction_opaque_data) {
        Worker.ExtraCatalog extra = extraCatalogOf(attach_opaque_data);
        if (extra != null) {
            if (!"main".equals(name)) return ItemsResponse.empty();
            return new ItemsResponse(List.of(RecordCodec.serializeToBytes(
                    new SchemaInfo(extra.schemaComment(), Map.of(), attach_opaque_data, name,
                            extraSchemaCounts(extra)))));
        }
        for (SchemaDesc s : workerSchemas()) {
            if (s.name.equals(name)) {
                return new ItemsResponse(List.of(RecordCodec.serializeToBytes(
                        new SchemaInfo(s.comment, s.tags, attach_opaque_data, name, schemaCounts(s)))));
            }
        }
        return ItemsResponse.empty();
    }

    /** Schema descriptors registered with the worker (default + auxiliary). */
    private record SchemaDesc(String name, String comment, Map<String, String> tags) {}

    /**
     * The auxiliary catalog an attach belongs to, or {@code null} for the main
     * catalog. Routed by the catalog name recorded at {@code catalog_attach};
     * under stdio / AF_UNIX the wire token IS the recorded plaintext id.
     */
    private Worker.ExtraCatalog extraCatalogOf(byte[] attachOpaqueData) {
        if (worker.extraCatalogs().isEmpty() || attachOpaqueData == null) return null;
        String name = catalogRegistry.catalogName(attachOpaqueData);
        if (name == null) {
            try {
                name = catalogRegistry.catalogName(sealer.unsealAttach(attachOpaqueData, null));
            } catch (RuntimeException sealedWithoutAuth) {
                return null;
            }
        }
        return name == null ? null : worker.extraCatalogs().get(name);
    }

    /**
     * Catalog tables owned by the auxiliary catalog of {@code attachPlain},
     * filtered to {@code schema}. Empty when the attach is not an extra catalog
     * or the catalog declares no tables.
     */
    private List<CatalogTable> extraCatalogTablesFor(byte[] attachPlain, String schema) {
        Worker.ExtraCatalog extra = extraCatalogOf(attachPlain);
        if (extra == null) return List.of();
        List<CatalogTable> all = worker.extraCatalogTables().get(extra.name());
        if (all == null) return List.of();
        List<CatalogTable> out = new ArrayList<>();
        for (CatalogTable t : all) {
            if (schema == null || t.schema().equals(schema)) out.add(t);
        }
        return out;
    }

    /** Whether a function name belongs to any registered auxiliary catalog. */
    private boolean ownedByExtraCatalog(String functionName) {
        for (Worker.ExtraCatalog extra : worker.extraCatalogs().values()) {
            if (functionName.startsWith(extra.functionNamePrefix())) return true;
        }
        return false;
    }

    /**
     * Object counts for an auxiliary catalog's single {@code main} schema: only
     * its owned table functions; zero counts let the C++ extension skip the
     * corresponding contents RPCs entirely.
     */
    private Map<String, Long> extraSchemaCounts(Worker.ExtraCatalog extra) {
        long owned = 0;
        for (var v : tables.values()) {
            for (TableFunction fn : v) {
                if (fn.name().startsWith(extra.functionNamePrefix())) owned++;
            }
        }
        for (var v : bufferingFns.values()) {
            for (var fn : v) {
                if (fn.name().startsWith(extra.functionNamePrefix())) owned++;
            }
        }
        List<CatalogTable> ownedTables = worker.extraCatalogTables().get(extra.name());
        long tableCount = ownedTables == null ? 0L : ownedTables.size();
        Map<String, Long> m = new java.util.LinkedHashMap<>();
        m.put("scalar_function", 0L);
        m.put("table_function", owned);
        m.put("aggregate_function", 0L);
        m.put("table", tableCount);
        m.put("view", 0L);
        m.put("macro", 0L);
        m.put("index", 0L);
        return m;
    }

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
        Map<String, Map<String, String>> tags = worker.schemaTags();
        String defaultSchema = worker.defaultSchema();
        result.add(new SchemaDesc(defaultSchema,
                comments.getOrDefault(defaultSchema, "Default schema"),
                tags.getOrDefault(defaultSchema, Map.of())));
        // The minimal `versioned` fixture exposes only its default schema
        // (vgi-python parity); the example worker's auxiliary `data` schema is
        // not part of it. attach/versioning_http.test asserts exactly one schema.
        if ("versioned".equals(worker.catalogName())) return result;
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
        for (String s : extras) {
            result.add(new SchemaDesc(s, comments.getOrDefault(s, ""),
                    tags.getOrDefault(s, Map.of())));
        }
        return result;
    }

    /**
     * Per-schema object counts. Keys are the C++ extension's
     * {@code VgiObjectCounts} field names (singular: {@code "table"},
     * {@code "view"}, {@code "macro"}, {@code "index"},
     * {@code "scalar_function"}, {@code "aggregate_function"},
     * {@code "table_function"}). When a key is {@code 0}, the C++ extension
     * treats it as a hard guarantee and skips the corresponding
     * {@code catalog_schema_contents_*} RPC entirely (see
     * {@code VgiCatalogSet::ShouldBypassRpcLocked}).
     */
    private Map<String, Long> schemaCounts(SchemaDesc s) {
        Map<String, Long> m = new java.util.LinkedHashMap<>();
        long scalarCount = 0;
        long tableFnCount = 0;
        long aggregateCount = 0;
        if (s.name.equals(worker.defaultSchema())) {
            for (var v : scalars.values()) scalarCount += v.size();
            for (var v : tables.values()) {
                for (TableFunction fn : v) {
                    if (!ownedByExtraCatalog(fn.name())) tableFnCount++;
                }
            }
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

    /**
     * List the user-visible tables in a schema, version-resolved for the attach's data version.
     *
     * @param attach_opaque_data the attach token
     * @param name the schema name
     * @param transaction_opaque_data the optional transaction token
     * @param ctx the per-call RPC context
     * @return one serialised {@code TableInfo} per visible table
     */
    @Override
    public ItemsResponse catalog_schema_contents_tables(
            byte[] attach_opaque_data, String name, byte[] transaction_opaque_data, CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        List<CatalogTable> extraTables = extraCatalogTablesFor(attach_opaque_data_plain, name);
        if (extraCatalogOf(attach_opaque_data_plain) != null) {
            List<byte[]> extraItems = new ArrayList<>();
            for (CatalogTable t : extraTables) {
                extraItems.add(TableInfoSerializer.serialize(toTableInfo(t)));
            }
            return new ItemsResponse(extraItems);
        }
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

    /**
     * Resolve the scan function (name + arguments) for a table, honouring an {@code AT} time-travel clause.
     *
     * @param attach_opaque_data the attach token
     * @param schema_name the table's schema
     * @param name the table name
     * @param at_unit the time-travel unit (e.g. {@code version}), or {@code null}
     * @param at_value the time-travel value, or {@code null}
     * @param transaction_opaque_data the optional transaction token
     * @param ctx the per-call RPC context
     * @return the scan-function response (function name + encoded arguments)
     * @throws IllegalArgumentException if the table is unknown
     */
    @Override
    public farm.query.vgi.protocol.TableScanFunctionGetResponse catalog_table_scan_function_get(
            byte[] attach_opaque_data, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_opaque_data, CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        if (extraCatalogOf(attach_opaque_data_plain) != null) {
            for (CatalogTable t : extraCatalogTablesFor(attach_opaque_data_plain, schema_name)) {
                if (t.name().equals(name) && t.scanFunctionName() != null) {
                    byte[] argsBytes = ScanFunctionResultEncoder.encodeArguments(
                            t.scanFunctionPositional() == null ? List.of() : t.scanFunctionPositional(),
                            t.scanFunctionNamed() == null ? Map.of() : t.scanFunctionNamed());
                    return new farm.query.vgi.protocol.TableScanFunctionGetResponse(
                            t.scanFunctionName(), argsBytes, List.of());
                }
            }
            throw new IllegalArgumentException("scan_function_get: unknown table " + schema_name + "." + name);
        }
        var at = catalogRegistry.effectiveAt(attach_opaque_data_plain, at_unit, at_value);
        // Columns-based time-travel + pushdown: resolve AT -> version and pass it
        // as a scan-function argument (the native columns-based AT mechanism).
        if ("data".equals(schema_name) && "tt_pushdown_cols".equals(name)) {
            byte[] argsBytes = ScanFunctionResultEncoder.encodeArguments(
                    List.of((Object) (long) resolveTtVersion(at.unit(), at.value())), Map.of());
            return new farm.query.vgi.protocol.TableScanFunctionGetResponse(
                    "tt_pushdown_cols_scan", argsBytes, List.of());
        }
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

    /**
     * Resolve the scan branches for a table — declared branches for multi-branch
     * tables, else a single-branch wrap of the regular scan function.
     *
     * @param attach_opaque_data the attach token
     * @param schema_name the table's schema
     * @param name the table name
     * @param at_unit the time-travel unit, or {@code null}
     * @param at_value the time-travel value, or {@code null}
     * @param transaction_opaque_data the optional transaction token
     * @param ctx the per-call RPC context
     * @return serialised {@code ScanBranchesResult} bytes
     * @throws IllegalArgumentException if the table is unknown
     */
    @Override
    public byte[] catalog_table_scan_branches_get(
            byte[] attach_opaque_data, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_opaque_data, CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        if (extraCatalogOf(attach_opaque_data_plain) != null) {
            for (CatalogTable t : extraCatalogTablesFor(attach_opaque_data_plain, schema_name)) {
                if (t.name().equals(name) && t.scanFunctionName() != null) {
                    farm.query.vgi.catalog.ScanBranch one = new farm.query.vgi.catalog.ScanBranch(
                            t.scanFunctionName(),
                            t.scanFunctionPositional() == null ? List.of() : t.scanFunctionPositional(),
                            t.scanFunctionNamed() == null ? Map.of() : t.scanFunctionNamed(),
                            null, false, null, null, null);
                    return ScanBranchesResultSerializer.serialize(List.of(one), List.of());
                }
            }
            throw new IllegalArgumentException(
                    "scan_branches_get: unknown table " + schema_name + "." + name);
        }
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
        // Columns-based time-travel + pushdown: resolve AT -> version and wrap
        // tt_pushdown_cols_scan(version) as the single branch.
        if ("data".equals(schema_name) && "tt_pushdown_cols".equals(name)) {
            farm.query.vgi.catalog.ScanBranch one = new farm.query.vgi.catalog.ScanBranch(
                    "tt_pushdown_cols_scan",
                    List.of((Object) (long) resolveTtVersion(at.unit(), at.value())),
                    Map.of(), null, false, null, null, null);
            return ScanBranchesResultSerializer.serialize(List.of(one), List.of());
        }
        for (CatalogTable t : worker.catalogTables()) {
            if (t.schema().equals(schema_name) && t.name().equals(name)) {
                CatalogTable resolved = catalogRegistry.resolveVersion(t, at.unit(), at.value());
                if (resolved.scanFunctionName() == null) break;
                farm.query.vgi.catalog.ScanBranch one = new farm.query.vgi.catalog.ScanBranch(
                        resolved.scanFunctionName(),
                        resolved.scanFunctionPositional() == null ? List.of() : resolved.scanFunctionPositional(),
                        resolved.scanFunctionNamed() == null ? Map.of() : resolved.scanFunctionNamed(),
                        null, false, null, null, null);
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

    /**
     * A storage facade scoped (and shard-pinned) to one attach, for state that
     * persists across queries within the ATTACH session; {@code null} when no
     * attach identity is available.
     */
    private farm.query.vgi.storage.BoundStorage attachScopedStorage(byte[] attachPlain) {
        if (attachPlain == null || attachPlain.length == 0) return null;
        return new farm.query.vgi.storage.BoundStorage(this.storage, attachPlain, attachPlain);
    }

    /**
     * Cold-load the buffering init metadata persisted at Sink init, so any
     * pool worker can serve a process/combine RPC (Python parity). Absent
     * metadata (legacy execution) degrades to empty args / null schema.
     */
    private BufferingInitState bufferingInitState(farm.query.vgi.storage.BoundStorage storage) {
        byte[] payload = storage.stateGet(farm.query.vgi.storage.FrameworkNs.BUFFERING_INIT,
                farm.query.vgi.storage.BoundStorage.packIntKey(-1));
        if (payload == null) return new BufferingInitState(null, null, null, null, null, null, null);
        return RecordCodec.deserializeFromBytes(payload, BufferingInitState.class);
    }

    /**
     * Sink phase: buffer one input batch into the function's append-log and return its state id.
     *
     * @param request the process request (function, execution id, input batch, batch index)
     * @param ctx the per-call RPC context (used for client logging)
     * @return the response carrying the appended state id
     */
    @Override
    public farm.query.vgi.protocol.TableBufferingProcessResponse table_buffering_process(
            farm.query.vgi.protocol.TableBufferingProcessRequest request, CallContext ctx) {
        var fn = bufferingFn(request.function_name());
        byte[] attachPlain = sealer.unsealAttach(request.attach_opaque_data(), authOf(ctx));
        farm.query.vgi.storage.BoundStorage storage = new farm.query.vgi.storage.BoundStorage(
                this.storage, request.execution_id(), attachPlain);
        BufferingInitState init = bufferingInitState(storage);
        var params = new farm.query.vgi.buffering.TableBufferingProcessParams(
                request.function_name(), request.execution_id(), storage, request.batch_index(), ctx,
                ArgumentsParser.parse(init.arguments()),
                init.output_schema() == null ? null : SchemaUtil.deserializeSchema(init.output_schema()),
                init.attach_plain() != null ? init.attach_plain() : attachPlain,
                init.input_schema() == null ? null : SchemaUtil.deserializeSchema(init.input_schema()),
                init.copy_to(), init.secrets());
        byte[] stateId;
        try (org.apache.arrow.vector.VectorSchemaRoot root =
                     BatchUtil.readSingleBatch(request.input_batch(), Allocators.root())) {
            stateId = fn.process(root, params);
        }
        return new farm.query.vgi.protocol.TableBufferingProcessResponse(stateId);
    }

    /**
     * Combine sink-phase state ids into the finalize-state ids the source phase will drain.
     *
     * @param request the combine request (function, execution id, sink state ids)
     * @param ctx the per-call RPC context (used for client logging)
     * @return the response carrying the finalize-state ids
     */
    @Override
    public farm.query.vgi.protocol.TableBufferingCombineResponse table_buffering_combine(
            farm.query.vgi.protocol.TableBufferingCombineRequest request, CallContext ctx) {
        var fn = bufferingFn(request.function_name());
        byte[] attachPlain = sealer.unsealAttach(request.attach_opaque_data(), authOf(ctx));
        farm.query.vgi.storage.BoundStorage storage = new farm.query.vgi.storage.BoundStorage(
                this.storage, request.execution_id(), attachPlain);
        BufferingInitState init = bufferingInitState(storage);
        var params = new farm.query.vgi.buffering.TableBufferingCombineParams(
                request.function_name(), request.execution_id(), storage, ctx,
                ArgumentsParser.parse(init.arguments()),
                init.output_schema() == null ? null : SchemaUtil.deserializeSchema(init.output_schema()),
                init.attach_plain() != null ? init.attach_plain() : attachPlain,
                init.input_schema() == null ? null : SchemaUtil.deserializeSchema(init.input_schema()),
                init.copy_to(), init.secrets());
        List<byte[]> ids = fn.combine(
                request.state_ids() == null ? List.of() : request.state_ids(), params);
        return new farm.query.vgi.protocol.TableBufferingCombineResponse(ids);
    }

    /**
     * Release all buffered state for an execution once its buffering pipeline is done.
     *
     * @param request the destructor request (execution id)
     * @param ctx the per-call RPC context
     * @return an empty acknowledgement
     */
    @Override
    public farm.query.vgi.protocol.TableBufferingDestructorResponse table_buffering_destructor(
            farm.query.vgi.protocol.TableBufferingDestructorRequest request, CallContext ctx) {
        new farm.query.vgi.storage.BoundStorage(this.storage, request.execution_id(),
                sealer.unsealAttach(request.attach_opaque_data(), authOf(ctx)))
                .executionClear();
        return new farm.query.vgi.protocol.TableBufferingDestructorResponse();
    }

    /**
     * Fetch a single table by name, version-resolved for any {@code AT} time-travel clause.
     *
     * @param attach_opaque_data the attach token
     * @param schema_name the table's schema
     * @param name the table name
     * @param at_unit the time-travel unit, or {@code null}
     * @param at_value the time-travel value, or {@code null}
     * @param transaction_opaque_data the optional transaction token
     * @param ctx the per-call RPC context
     * @return a one-item response, or empty when the table is unknown / hidden
     */
    @Override
    public ItemsResponse catalog_table_get(
            byte[] attach_opaque_data, String schema_name, String name,
            String at_unit, String at_value, byte[] transaction_opaque_data, CallContext ctx) {
        byte[] attach_opaque_data_plain = sealer.unsealAttach(attach_opaque_data, authOf(ctx));
        if (extraCatalogOf(attach_opaque_data_plain) != null) {
            for (CatalogTable t : extraCatalogTablesFor(attach_opaque_data_plain, schema_name)) {
                if (t.name().equals(name)) {
                    return new ItemsResponse(List.of(TableInfoSerializer.serialize(toTableInfo(t))));
                }
            }
            return ItemsResponse.empty();
        }
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

    /**
     * Per-column statistics for a catalog table (used when not inlined into {@code TableInfo}).
     *
     * @param attach_opaque_data the attach token
     * @param schema_name the table's schema
     * @param name the table name
     * @param transaction_opaque_data the optional transaction token
     * @param ctx the per-call RPC context
     * @return serialised {@code ColumnStatistics}, or empty bytes when the table has none
     */
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
                null,
                t.requiredFieldFilterPaths() == null ? List.of() : t.requiredFieldFilterPaths());
    }

    /**
     * List the views in a schema.
     *
     * @param attach_opaque_data the attach token
     * @param name the schema name
     * @param transaction_opaque_data the optional transaction token
     * @return one serialised {@code ViewInfo} per view
     */
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

    /**
     * Fetch a single view by name.
     *
     * @param attach_opaque_data the attach token
     * @param schema_name the view's schema
     * @param name the view name
     * @param transaction_opaque_data the optional transaction token
     * @return a one-item response, or empty when the view is unknown
     */
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

    /**
     * List the macros in a schema, filtered by macro type.
     *
     * @param attach_opaque_data the attach token
     * @param name the schema name
     * @param type macro type filter ({@code scalar} / {@code table}); {@code null} returns both
     * @param transaction_opaque_data the optional transaction token
     * @return one serialised {@code MacroInfo} per matching macro
     */
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

    /**
     * Fetch a single macro by name.
     *
     * @param attach_opaque_data the attach token
     * @param schema_name the macro's schema
     * @param name the macro name
     * @param transaction_opaque_data the optional transaction token
     * @return a one-item response, or empty when the macro is unknown
     */
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
        // arguments_schema: one nullable field per parameter (in order), field
        // type from the default-value literal when known else Arrow null, and a
        // vgi_doc field-metadata entry for each documented parameter. Last in
        // the MacroInfo field order; additive + optional.
        byte[] argumentsSchema = MacroArgumentsSchema.toIpcBytes(
                m.parameters(), m.parameterDefaults(), m.parameterDocs());
        return new farm.query.vgi.protocol.MacroInfo(
                m.comment(), m.tags(), m.name(), m.schema(), macroType,
                m.parameters(), defaults, m.definition(), argumentsSchema);
    }

    /**
     * List the functions in the default schema, filtered by function type.
     * Table, table-in-out and buffering functions all surface as table functions.
     *
     * @param attach_opaque_data the attach token
     * @param name the schema name (only the default schema carries functions)
     * @param type function type filter ({@code scalar} / {@code table} / {@code aggregate}); {@code null} returns all
     * @param transaction_opaque_data the optional transaction token
     * @param ctx the per-call RPC context
     * @return one serialised {@code FunctionInfo} per matching function/overload
     */
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
        Worker.ExtraCatalog extraOnlyTables = extraCatalogOf(attach_opaque_data_plain);
        if (wantScalar && extraOnlyTables == null) {
            for (List<ScalarFunction> variants : scalars.values()) {
                for (ScalarFunction fn : variants) {
                    items.add(FunctionInfoSerializer.serialize(toScalarFunctionInfo(fn, name)));
                }
            }
        }
        if (wantTable) {
            String attachCatName = catalogRegistry.catalogName(attach_opaque_data_plain);
            boolean isProjReproAttach = "projection_repro".equals(attachCatName);
            // MetaWorker-style routing: an auxiliary catalog's attach sees only
            // its owned (prefix-matched) functions; the main catalog hides them.
            Worker.ExtraCatalog extraAttach = attachCatName == null
                    ? null : worker.extraCatalogs().get(attachCatName);
            for (List<TableFunction> variants : tables.values()) {
                for (TableFunction fn : variants) {
                    // proj_repro_* fixtures live in the example worker binary
                    // but only belong to the projection_repro catalog. Show
                    // them only when that catalog was attached.
                    if (!isProjReproAttach && fn.name().startsWith("proj_repro_")) continue;
                    if (isProjReproAttach && !fn.name().startsWith("proj_repro_")) continue;
                    if (extraAttach == null && ownedByExtraCatalog(fn.name())) continue;
                    if (extraAttach != null && !fn.name().startsWith(extraAttach.functionNamePrefix())) continue;
                    items.add(FunctionInfoSerializer.serialize(toTableFunctionInfo(fn, name)));
                }
            }
            // Table-in-out functions also register as function_type='table'
            // (DuckDB doesn't distinguish them at the catalog level — both
            // come back as TableFunction nodes; the bind/init flow tells them
            // apart by argument shape).
            for (List<TableInOutFunction> variants : tableInOuts.values()) {
                for (TableInOutFunction fn : variants) {
                    if (extraAttach == null && ownedByExtraCatalog(fn.name())) continue;
                    if (extraAttach != null && !fn.name().startsWith(extraAttach.functionNamePrefix())) continue;
                    items.add(FunctionInfoSerializer.serialize(toTableInOutFunctionInfo(fn, name)));
                }
            }
            // Buffering (Sink+Source) functions also surface as table functions;
            // their FunctionInfo.function_type="table_buffering" selects the
            // C++ buffering operator.
            for (var variants : bufferingFns.values()) {
                for (var fn : variants) {
                    if (extraAttach == null && ownedByExtraCatalog(fn.name())) continue;
                    if (extraAttach != null && !fn.name().startsWith(extraAttach.functionNamePrefix())) continue;
                    items.add(FunctionInfoSerializer.serialize(toBufferingFunctionInfo(fn, name)));
                }
            }
        }
        if (wantAggregate && extraOnlyTables == null) {
            for (List<AggregateFunction<?>> variants : aggregates.values()) {
                for (AggregateFunction<?> fn : variants) {
                    items.add(FunctionInfoSerializer.serialize(toAggregateFunctionInfo(fn, name)));
                }
            }
        }
        return new ItemsResponse(items);
    }

    /**
     * List the custom {@code COPY ... FROM} formats advertised by this catalog
     * (catalog-level, not schema-scoped). Introspects the registered table
     * functions for {@link farm.query.vgi.table.CopyFromFunction} instances and
     * converts each into a {@code CopyFromFormatInfo}. Mirrors vgi-python's
     * {@code ReadOnlyCatalogInterface.copy_from_formats}: the option schema
     * reuses the same argument serialization as the function-enumeration path,
     * so option types / defaults / {@code vgi_doc} descriptions surface
     * identically to {@code vgi_function_arguments()}.
     *
     * <p>Auxiliary (prefix-owned) catalogs advertise no COPY formats — the
     * format readers belong to the main catalog only.
     *
     * @param attach_opaque_data the attach token
     * @param transaction_opaque_data the optional transaction token
     * @return one serialised {@code CopyFromFormatInfo} per registered format reader
     */
    @Override
    public ItemsResponse catalog_copy_from_formats(byte[] attach_opaque_data,
                                                    byte[] transaction_opaque_data) {
        // COPY formats are owned by the main catalog; an auxiliary catalog's
        // attach advertises none (parallels catalog_schemas' extra-catalog gate).
        if (extraCatalogOf(attach_opaque_data) != null) return ItemsResponse.empty();
        boolean projReproAttach =
                "projection_repro".equals(catalogRegistry.catalogName(attach_opaque_data));
        List<byte[]> items = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (List<TableFunction> variants : tables.values()) {
            for (TableFunction fn : variants) {
                if (!(fn instanceof farm.query.vgi.table.CopyFromFunction cf)) continue;
                // Same per-catalog ownership rules as the function listing.
                if (!projReproAttach && fn.name().startsWith("proj_repro_")) continue;
                if (projReproAttach && !fn.name().startsWith("proj_repro_")) continue;
                if (!projReproAttach && ownedByExtraCatalog(fn.name())) continue;
                String format = cf.copyFromFormat();
                String direction = cf.copyFromDirection();
                if (format == null || format.isEmpty() || !seen.add(direction + ":" + format)) continue;
                farm.query.vgi.function.FunctionMetadata md = fn.metadata();
                items.add(CopyFromFormatInfoSerializer.serialize(
                        new farm.query.vgi.protocol.CopyFromFormatInfo(
                                cf.copyFromComment(),
                                md.tags() == null ? Map.of() : md.tags(),
                                format,
                                fn.name(),
                                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()),
                                direction,
                                md.description() == null ? "" : md.description(),
                                /*ordered=*/false)));
            }
        }
        // COPY ... TO writers are TableBufferingFunctions; enumerate them from
        // the buffering registry, mirroring vgi-python's copy_from_formats
        // (the RPC name is historical — it returns all directions). The reader
        // and writer for a shared format name are keyed by direction.
        for (List<farm.query.vgi.buffering.TableBufferingFunction> variants : bufferingFns.values()) {
            for (farm.query.vgi.buffering.TableBufferingFunction fn : variants) {
                if (!(fn instanceof farm.query.vgi.table.CopyToFunction ct)) continue;
                if (!projReproAttach && ownedByExtraCatalog(fn.name())) continue;
                String format = ct.copyToFormat();
                String direction = ct.copyToDirection();
                if (format == null || format.isEmpty() || !seen.add(direction + ":" + format)) continue;
                farm.query.vgi.function.FunctionMetadata md = fn.metadata();
                items.add(CopyFromFormatInfoSerializer.serialize(
                        new farm.query.vgi.protocol.CopyFromFormatInfo(
                                ct.copyToComment(),
                                md.tags() == null ? Map.of() : md.tags(),
                                format,
                                fn.name(),
                                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()),
                                direction,
                                md.description() == null ? "" : md.description(),
                                /*ordered=*/fn.sinkOrderDependent())));
            }
        }
        return new ItemsResponse(items);
    }

    private FunctionInfo toScalarFunctionInfo(ScalarFunction fn, String schemaName) {
        return scalarFunctionInfo(fn, schemaName);
    }

    /**
     * Build the {@code FunctionInfo} for a scalar function exactly as the
     * catalog-enumeration path does (binds with empty args, maps metadata).
     * Package-private + static so it is exercisable in isolation by tests
     * without standing up a full server.
     *
     * @param fn the scalar function.
     * @param schemaName the owning schema name.
     * @return the wire {@link FunctionInfo}.
     */
    static FunctionInfo scalarFunctionInfo(ScalarFunction fn, String schemaName) {
        BindResponse r = fn.onBind(new ScalarBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        return baseFunctionInfo(fn, schemaName, "scalar", bindOutput(r), false);
    }

    private FunctionInfo toTableFunctionInfo(TableFunction fn, String schemaName) {
        if (fn instanceof farm.query.vgi.table.CopyFromFunction) {
            // COPY-FROM readers have no static output schema — it's the COPY
            // target's columns, supplied per-statement via the copy_from bind
            // context. Calling onBind here would (intentionally) throw, so
            // advertise an empty output schema, mirroring vgi-python's
            // _function_to_info (which emits an empty schema for table funcs).
            return baseFunctionInfo(fn, schemaName, "table",
                    SchemaUtil.serializeSchema(new Schema(List.of())), false);
        }
        BindResponse r = fn.onBind(new TableBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        return baseFunctionInfo(fn, schemaName, "table", bindOutput(r), false);
    }

    private FunctionInfo toTableInOutFunctionInfo(TableInOutFunction fn, String schemaName) {
        BindResponse r = fn.onBind(new TableInOutBindParams(fn.name(), Arguments.empty(), null, Map.of()));
        // Exchange-only: TIO has no finalize phase (Sink+Source lives in the
        // buffering interface), so DuckDB never issues a FINALIZE-phase RPC.
        return baseFunctionInfo(fn, schemaName, "table", bindOutput(r), /*hasFinalize=*/false);
    }

    private FunctionInfo toAggregateFunctionInfo(AggregateFunction<?> fn, String schemaName) {
        FunctionInfo base = baseFunctionInfo(fn, schemaName, "aggregate",
                SchemaUtil.serializeSchema(fn.outputSchema()), false);
        var required = fn.requiredSecrets();
        if (required == null || required.isEmpty()) return base;
        // Re-stamp required_secrets (baseFunctionInfo hardcodes an empty list);
        // the C++ extension pre-resolves these and delivers them on
        // AggregateBindRequest.secrets.
        return new FunctionInfo(
                base.comment(), base.tags(), base.name(), base.schema_name(), base.function_type(),
                base.arguments(), base.output_schema(), base.stability(), base.null_handling(),
                base.description(), base.examples(), base.categories(), base.projection_pushdown(),
                base.filter_pushdown(), base.sampling_pushdown(), base.late_materialization(),
                base.supported_expression_filters(),
                base.order_preservation(), base.max_workers(), base.supports_batch_index(),
                base.partition_kind(), base.order_dependent(), base.distinct_dependent(),
                base.supports_window(), base.streaming_partitioned(), base.has_finalize(),
                base.source_order_dependent(), base.sink_order_dependent(),
                base.requires_input_batch_index(),
                base.required_settings(), required);
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

    private static FunctionInfo baseFunctionInfo(farm.query.vgi.function.FunctionDescriptor fn,
                                           String schemaName, String type,
                                           byte[] outputSchema, boolean hasFinalize) {
        // Only TableFunction has a meaningful maxWorkers; everything else
        // is single-worker by definition.
        int maxWorkers = fn instanceof TableFunction tf ? (int) tf.maxWorkers() : 1;
        return baseFunctionInfo(fn.name(), fn.metadata(), schemaName, type,
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()), outputSchema, hasFinalize,
                maxWorkers);
    }

    private static FunctionInfo baseFunctionInfo(String name, FunctionMetadata md, String schemaName,
                                           String type, byte[] arguments, byte[] outputSchema,
                                           boolean hasFinalize, int maxWorkers) {
        return new FunctionInfo(
                md.description().isEmpty() ? null : md.description(),
                md.tags() == null ? Map.of() : md.tags(),
                name,
                schemaName,
                type,
                arguments,
                outputSchema,
                stabilityWire(md.stability()),
                (BAD_ENUM_MODE && "scalar".equals(type) && "double".equals(name))
                        ? "WEIRD" : nullHandlingWire(md.nullHandling()),
                md.description(),
                md.examples() == null ? List.of() : md.examples(),
                md.categories() == null ? List.of() : md.categories(),
                md.projectionPushdown(),
                md.filterPushdown() ? Boolean.TRUE : null,
                md.samplingPushdown() ? Boolean.TRUE : null,
                md.lateMaterialization() ? Boolean.TRUE : null,
                md.supportedExpressionFilters() == null ? List.of() : md.supportedExpressionFilters(),
                md.orderPreservation() == null ? null : md.orderPreservation().wireName(),
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
