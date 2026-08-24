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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code multi_batch_finish(data TABLE)} — a streaming FINALIZE that emits
 * MANY batches.
 *
 * <p>Every other finalize fixture, in every SDK, emits exactly ONE batch — and
 * one batch is the easy case: over HTTP a producer is strictly lock-step, so a
 * single-batch flush completes inside its one turn and never needs a
 * continuation. Two or more do, and that path was broken in two independent
 * places (the Rust worker's flush producer and the DuckDB client's finalize
 * drain) for as long as no fixture emitted a second batch.
 *
 * <p>It emits one batch per input row the substream saw: the first carries that
 * substream's total, the rest carry 0. The split makes the two failure modes
 * tell themselves apart — a wrong {@code SUM} means a batch's CONTENTS were
 * lost or duplicated, a wrong {@code COUNT} means a whole BATCH was. The second
 * is what catches a truncated flush, because the rows that do arrive are
 * correct and only the count betrays the missing ones.
 *
 * <p>Both invariants hold at any substream fan-out, so the SQL test needs no
 * assumption about thread count. Mirrors vgi-python's
 * {@code MultiBatchFinishFunction}; backs
 * {@code vgi/test/sql/integration/table_in_out/multi_batch_finalize.test}.
 */
public final class MultiBatchFinishFunction implements TableInOutFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("multi_batch_finish")
            .metadata(FunctionMetadata.describe(
                            "Streaming finalize that emits one batch per input row (multi-batch flush)")
                    .withCategories("testing", "aggregation"))
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
     * a per-exchange id the state can serialize. Same mechanism as
     * {@link SubstreamPartialSumFunction}: a {@code BoundStorage} wraps a live
     * SQLite connection and cannot ride an HTTP continuation token, but the id
     * can, and the state re-resolves the view from here on the other side.
     */
    private static final ConcurrentHashMap<String, BoundStorage> LIVE_STORAGE =
            new ConcurrentHashMap<>();

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        String key = UUID.randomUUID().toString();
        LIVE_STORAGE.put(key, params.storage());
        return new State(key, params.outputSchema());
    }

    /**
     * Emit one batch per input row this substream saw — the first carrying the
     * substream's total, the rest zero. An input-less substream emits nothing.
     */
    @Override public List<VectorSchemaRoot> finish(TableInOutInitParams params) {
        long total = 0;
        long rows = 0;
        for (FunctionStorage.KV kv : params.storage().stateDrain(FrameworkNs.TIO_STATE)) {
            ByteBuffer buf = ByteBuffer.wrap(kv.value()).order(ByteOrder.LITTLE_ENDIAN);
            total += buf.getLong();
            rows += buf.getLong();
        }
        List<VectorSchemaRoot> out = new ArrayList<>();
        for (long i = 0; i < rows; i++) {
            VectorSchemaRoot root = VectorSchemaRoot.create(params.outputSchema(), Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, i == 0 ? total : 0L);
            root.setRowCount(1);
            out.add(root);
        }
        return out;
    }

    /** Accumulate column-0 sums and the row count; persist both after every batch. */
    public static final class State extends TableInOutExchangeState {
        /** Key into {@link #LIVE_STORAGE}; survives the state token. */
        public String storageKey;
        /** Emit schema (Arrow schemas ride the token as IPC bytes). */
        public Schema outputSchema;
        /** Running sum of column 0 across the batches seen so far. */
        public long total;
        /** Running count of rows seen so far — this is the batch count finish() emits. */
        public long rows;
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
                            "multi_batch_finish: no live storage for exchange " + storageKey
                            + " (state resumed in a different worker process?)");
                }
            }
            return storageRef;
        }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot in = input.root();
            FieldVector col = in.getVector(0);
            int n = in.getRowCount();
            for (int i = 0; i < n; i++) {
                if (!col.isNull(i)) total += ScalarHelpers.toLong(col, i);
            }
            rows += n;
            // Upsert (total, rows) keyed per worker process; finish() drains and
            // sums every entry.
            byte[] value = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(total).putLong(rows).array();
            storage().statePut(FrameworkNs.TIO_STATE,
                    BoundStorage.packIntKey(ProcessHandle.current().pid()), value);
            VectorSchemaRoot empty = VectorSchemaRoot.create(outputSchema, Allocators.root());
            empty.setRowCount(0);
            out.emit(empty);
        }
    }
}
