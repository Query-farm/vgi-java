// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.storage.FrameworkNs;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code substream_partial_sum(data TABLE)} — per-substream partial sum emitted
 * at finalize; proves parallel streaming FINALIZE (Phase A4).
 *
 * <p>A streaming table-in-out <em>with</em> a finalize is still a per-substream
 * operation under per-substream worker fan-out: the exchange accumulates only
 * THIS substream's rows (emitting empty batches), and {@link #finish} emits ONE
 * row = this substream's partial sum. DuckDB fans the input across N workers
 * and unions their finalize outputs, so the caller re-aggregates with an outer
 * {@code SELECT sum(...)} — correct no matter how the rows were partitioned.
 * State is coordinated through execution-scoped storage (the FINALIZE init
 * carries the INPUT phase's {@code execution_id});
 * {@code params.substreamId()} is the stable client-owned key available for
 * workers that manage cross-backend state themselves. This is NOT a global
 * cross-substream combine (that is a {@code TableBufferingFunction}; see
 * {@code sum_all_columns_simple_distributed}). Mirrors vgi-python's
 * {@code SubstreamPartialSumFunction}.
 */
public final class SubstreamPartialSumFunction implements TableInOutFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("substream_partial_sum")
            .metadata(FunctionMetadata.describe(
                            "Per-substream partial sum emitted at finalize (parallel streaming finalize)")
                    .withCategories("aggregation", "numeric"))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public boolean hasFinalize() { return true; }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null || in.getFields().isEmpty()) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        String name = in.getFields().get(0).getName();
        return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of(
                new Field(name, new FieldType(true, Schemas.INT64, null), null)))));
    }

    /**
     * Live storage views for the exchanges in flight in THIS process, keyed by
     * a per-exchange id the state can serialize. A {@link BoundStorage} wraps a
     * live SQLite connection, so it cannot ride an HTTP continuation token; the
     * id can, and the state re-resolves the view from here on the other side.
     * Same shape as {@code CacheParallelFunctions}' per-execution work queues
     * ({@code execKey} + a transient reference).
     */
    private static final ConcurrentHashMap<String, BoundStorage> LIVE_STORAGE =
            new ConcurrentHashMap<>();

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        String key = UUID.randomUUID().toString();
        LIVE_STORAGE.put(key, params.storage());
        return new State(key, params.outputSchema());
    }

    /**
     * Emit this substream's partial sum: the sum of every running total the
     * exchange persisted into this execution's {@code TIO_STATE} namespace
     * (one entry per exchange state that handled this substream's batches).
     * An input-less substream drained nothing and emits a zero row.
     */
    @Override public List<VectorSchemaRoot> finish(TableInOutInitParams params) {
        long total = 0;
        for (FunctionStorage.KV kv : params.storage().stateDrain(FrameworkNs.TIO_STATE)) {
            total += ByteBuffer.wrap(kv.value()).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(params.outputSchema(), Allocators.root());
        root.allocateNew();
        ((BigIntVector) root.getVector(0)).setSafe(0, total);
        root.setRowCount(1);
        return List.of(root);
    }

    /** Accumulate column-0 sums; persist the running total after every batch.
     *
     *  <p>Serializable across an HTTP state-token round-trip: the running total
     *  and the output schema ride the token, and the storage view is
     *  re-resolved from {@link #LIVE_STORAGE} by a key that does. That keeps the
     *  rehydration process-local, which is what the fixture needs — the
     *  accumulated partial sums live in this worker's execution-scoped storage
     *  anyway, so a state resumed in a *different* process could not see them
     *  whatever we serialized. */
    public static final class State extends TableInOutExchangeState {
        /** Key into {@link #LIVE_STORAGE}; survives the state token. */
        public String storageKey;
        /** Emit schema (Arrow schemas ride the token as IPC bytes). */
        public Schema outputSchema;
        /** Running sum of column 0 across the batches seen so far. */
        public long total;
        /** Re-resolved on first use after a token round-trip. */
        private transient BoundStorage storageRef;

        /** No-arg constructor for HTTP state-token deserialization. */
        public State() {}

        State(String storageKey, Schema outputSchema) {
            this.storageKey = storageKey;
            this.outputSchema = outputSchema;
        }

        private BoundStorage storage() {
            if (storageRef == null) {
                storageRef = LIVE_STORAGE.get(storageKey);
                if (storageRef == null) {
                    throw new IllegalStateException(
                            "substream_partial_sum: no live storage for exchange " + storageKey
                            + " (state resumed in a different worker process?)");
                }
            }
            return storageRef;
        }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot in = input.root();
            FieldVector col = in.getVector(0);
            int rows = in.getRowCount();
            for (int i = 0; i < rows; i++) {
                if (!col.isNull(i)) total += ScalarHelpers.toLong(col, i);
            }
            // Upsert the running total keyed like vgi-python (one entry per
            // worker process); finish() drains and sums every entry.
            byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(total).array();
            storage().statePut(FrameworkNs.TIO_STATE,
                    BoundStorage.packIntKey(ProcessHandle.current().pid()), value);
            // Accumulate only; the exchange contract wants one (possibly
            // empty) output batch per input batch.
            VectorSchemaRoot empty = VectorSchemaRoot.create(outputSchema, Allocators.root());
            empty.setRowCount(0);
            out.emit(empty);
        }
    }
}
