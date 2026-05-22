// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionMetadata.OrderPreservation;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.EmitMetadata;
import farm.query.vgi.internal.HexId;
import farm.query.vgi.internal.SchemaUtil;
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
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * {@code supports_batch_index} reference fixtures. Each opts into FIXED_ORDER
 * preservation + per-batch {@code vgi_batch_index} tagging: the worker enqueues
 * {@code (partition_id, start, end)} chunks at bind time, every parallel scan
 * worker drains the shared queue, and DuckDB's ordered sinks reassemble output
 * in partition-id order. Mirrors vgi-python's {@code batch_index.py}.
 */
public final class BatchIndexFunctions {

    private BatchIndexFunctions() {}

    /** Per-execution work queue, shared across this execution's scan workers. */
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<long[]>> QUEUES =
            new ConcurrentHashMap<>();

    private static ConcurrentLinkedQueue<long[]> buildQueue(String key, long count, long chunkSize) {
        return QUEUES.computeIfAbsent(key, k -> {
            ConcurrentLinkedQueue<long[]> q = new ConcurrentLinkedQueue<>();
            long partitionId = 0;
            for (long start = 0; start < count; start += chunkSize) {
                q.add(new long[] {partitionId++, start, Math.min(start + chunkSize, count)});
            }
            return q;
        });
    }

    // =====================================================================
    // partitioned_batch_index(count) -> n int64
    // =====================================================================

    public static final class PartitionedBatchIndex implements TableFunction {

        private static final long CHUNK_SIZE = 1000L;
        private static final long BATCH_SIZE = 1000L;
        private static final Schema OUTPUT = Schemas.of(Schemas.nullable("n", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        private static final FunctionSpec SPEC = FunctionSpec.builder("partitioned_batch_index")
                .metadata(FunctionMetadata.describe(
                        "Multi-worker partitioned sequence with per-batch batch_index tagging; parallel scan + ordered sink reassembly.")
                        .withPushdown(true, false, false)
                        .withCategories("generator", "utility")
                        .withOrderPreservation(OrderPreservation.FIXED_ORDER)
                        .withBatchIndex())
                .constArg("count", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(OUTPUT_IPC);
        }

        @Override public long cardinality(TableBindParams p) {
            Object c = p.arguments().positionalAt(0);
            return c instanceof Number n ? n.longValue() : -1L;
        }

        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            long count = ParameterExtractor.of(p.arguments())
                    .positional(0, "count").asLong().required();
            String key = HexId.encode(p.executionId());
            return new State(buildQueue(key, count, CHUNK_SIZE), key);
        }

        public static final class State extends TableProducerState {
            public String execKey;
            public transient ConcurrentLinkedQueue<long[]> queueRef;
            public long partitionId = -1;
            public long currentEnd = -1;
            public long currentIdx = 0;
            public boolean haveChunk = false;

            public State() {}

            State(ConcurrentLinkedQueue<long[]> queue, String execKey) {
                this.queueRef = queue;
                this.execKey = execKey;
            }

            private ConcurrentLinkedQueue<long[]> queue() {
                if (queueRef == null) queueRef = QUEUES.get(execKey);
                return queueRef;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (!haveChunk || currentIdx >= currentEnd) {
                    long[] item = queue() == null ? null : queue().poll();
                    if (item == null) { out.finish(); return; }
                    partitionId = item[0];
                    currentIdx = item[1];
                    currentEnd = item[2];
                    haveChunk = true;
                }
                int n = (int) Math.min(BATCH_SIZE, currentEnd - currentIdx);
                long startIdx = currentIdx;
                BatchUtil.emit(OUTPUT, n, out, EmitMetadata.batchIndex(partitionId),
                        (root, rows, start) -> {
                    BigIntVector v = (BigIntVector) root.getVector("n");
                    for (int i = 0; i < rows; i++) v.setSafe(i, startIdx + i);
                });
                currentIdx += n;
            }
        }
    }

    // =====================================================================
    // partitioned_batch_index_marked(count, chunk_size := 1000)
    //   -> (partition_id int64, seq int64)
    // =====================================================================

    public static final class PartitionedBatchIndexMarked implements TableFunction {

        private static final long BATCH_SIZE = 256L;
        private static final Schema OUTPUT = Schemas.of(
                Schemas.nullable("partition_id", Schemas.INT64),
                Schemas.nullable("seq", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        // projection_pushdown stays OFF so partition_id survives SELECT seq.
        private static final FunctionSpec SPEC = FunctionSpec.builder("partitioned_batch_index_marked")
                .metadata(FunctionMetadata.describe(
                        "Two-column batch_index demo: rows are (partition_id, seq). Tests assert that DuckDB's ordered sinks reassemble output in partition_id order under parallel execution.")
                        .withCategories("generator", "utility", "testing")
                        .withOrderPreservation(OrderPreservation.FIXED_ORDER)
                        .withBatchIndex())
                .constArg("count", Schemas.INT64)
                .named("chunk_size", Schemas.INT64, "1000")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(OUTPUT_IPC);
        }

        @Override public long cardinality(TableBindParams p) {
            Object c = p.arguments().positionalAt(0);
            return c instanceof Number n ? n.longValue() : -1L;
        }

        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long count = ex.positional(0, "count").asLong().required();
            long chunkSize = ex.named("chunk_size").asLong().orElse(1000L);
            String key = HexId.encode(p.executionId()) + ":" + chunkSize;
            return new State(buildQueue(key, count, chunkSize), key);
        }

        public static final class State extends TableProducerState {
            public String execKey;
            public transient ConcurrentLinkedQueue<long[]> queueRef;
            public long partitionId = -1;
            public long currentStart = -1;
            public long currentEnd = -1;
            public long currentIdx = 0;
            public boolean haveChunk = false;

            public State() {}

            State(ConcurrentLinkedQueue<long[]> queue, String execKey) {
                this.queueRef = queue;
                this.execKey = execKey;
            }

            private ConcurrentLinkedQueue<long[]> queue() {
                if (queueRef == null) queueRef = QUEUES.get(execKey);
                return queueRef;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (!haveChunk || currentIdx >= currentEnd) {
                    long[] item = queue() == null ? null : queue().poll();
                    if (item == null) { out.finish(); return; }
                    partitionId = item[0];
                    currentStart = item[1];
                    currentIdx = item[1];
                    currentEnd = item[2];
                    haveChunk = true;
                }
                int n = (int) Math.min(BATCH_SIZE, currentEnd - currentIdx);
                long startIdx = currentIdx;
                long localStart = currentStart;
                long pidVal = partitionId;
                BatchUtil.emit(OUTPUT, n, out, EmitMetadata.batchIndex(partitionId),
                        (root, rows, start) -> {
                    BigIntVector pid = (BigIntVector) root.getVector("partition_id");
                    BigIntVector seq = (BigIntVector) root.getVector("seq");
                    for (int i = 0; i < rows; i++) {
                        pid.setSafe(i, pidVal);
                        seq.setSafe(i, startIdx - localStart + i);
                    }
                });
                currentIdx += n;
            }
        }
    }
}
