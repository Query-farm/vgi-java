// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * {@code partitioned_sequence(count BIGINT [const], increment := 1)} —
 * multi-worker sequence generator backed by a shared work queue keyed
 * by per-execution id. The first {@code createProducer} call for an
 * execution populates the queue with {@code [start, end)} chunks; every
 * worker (including the first) drains chunks from the queue until empty.
 * The union of all workers' output produces the complete sequence.
 *
 * <p>Multi-threaded correctness relies on each DuckDB scan thread getting
 * its own connection (i.e. its own JVM-side dispatch + producer state),
 * gated by {@link #maxWorkers}. The work queue lives in a {@code
 * ConcurrentHashMap<execId, ConcurrentLinkedQueue<long[]>>} so all
 * producers pull from the same queue without further synchronization.
 */
public final class PartitionedSequenceFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("n", new FieldType(true, Schemas.INT64, null), null)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);
    private static final long CHUNK = 10_000L;

    /** Per-execution work queue. Each entry is a {@code [start, end)} chunk. */
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<long[]>> QUEUES =
            new ConcurrentHashMap<>();

    private static String key(byte[] executionId) { return farm.query.vgi.internal.HexId.encode(executionId); }

    @Override public String name() { return "partitioned_sequence"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates a partitioned sequence for multi-worker execution");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true),
                ArgSpec.named("increment", Schemas.INT64, "1"));
    }
    @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(OUTPUT_SCHEMA_IPC); }

    @Override public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }

    /** Allow up to 8 concurrent scan workers; DuckDB clamps to its thread pool. */
    @Override public long maxWorkers() { return 8L; }

    @Override public TableProducerState createProducer(TableInitParams p) {
        long count = ((Number) p.arguments().positionalAt(0)).longValue();
        long increment = p.arguments().namedLong("increment", 1L);
        String execKey = key(p.executionId());

        ConcurrentLinkedQueue<long[]> queue = QUEUES.computeIfAbsent(execKey, k -> {
            ConcurrentLinkedQueue<long[]> q = new ConcurrentLinkedQueue<>();
            for (long start = 0; start < count; start += CHUNK) {
                long end = Math.min(start + CHUNK, count);
                q.add(new long[] {start, end});
            }
            return q;
        });
        return new State(queue, increment, execKey);
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String execKey;
        public long increment;
        public transient ConcurrentLinkedQueue<long[]> queueRef;
        // Current chunk being emitted in 1024-row sub-batches.
        public long currentStart = -1;
        public long currentEnd = -1;
        public long currentIdx = 0;

        public State() {}

        State(ConcurrentLinkedQueue<long[]> queue, long increment, String execKey) {
            this.queueRef = queue;
            this.increment = increment;
            this.execKey = execKey;
        }

        private ConcurrentLinkedQueue<long[]> queue() {
            if (queueRef == null) queueRef = QUEUES.get(execKey);
            return queueRef;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            // If we're done with the current chunk, pull the next one.
            if (currentIdx >= currentEnd) {
                long[] next = queue() == null ? null : queue().poll();
                if (next == null) {
                    // Don't drop the queue entry — concurrent createProducer
                    // calls would re-populate it (race seen during testing
                    // count=1 with maxWorkers > 1). Empty queues sit until
                    // the JVM exits; small per-execution overhead.
                    out.finish();
                    return;
                }
                currentStart = next[0];
                currentEnd = next[1];
                currentIdx = currentStart;
            }
            int n = (int) Math.min(2048L, currentEnd - currentIdx);
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            for (int i = 0; i < n; i++) v.setSafe(i, (currentIdx + i) * increment);
            root.setRowCount(n);
            out.emit(root);
            currentIdx += n;
        }
    }
}
