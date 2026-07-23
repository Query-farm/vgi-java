// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionMetadata.OrderPreservation;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.EmitMetadata;
import farm.query.vgi.internal.HexId;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Multi-worker / order-sensitive result-cache fixtures. Each fans work out over
 * a per-execution queue so a cached scan captures one substream per worker
 * thread; {@code cache_ordered} additionally tags each batch with
 * {@code vgi_batch_index} so the cache's serve path must reassemble source
 * order. Mirrors vgi-python's {@code cache.py}.
 */
public final class CacheParallelFunctions {

    private CacheParallelFunctions() {}

    /** Per-execution work queue; each entry is {@code [partitionId, start, end)}. */
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<long[]>> QUEUES =
            new ConcurrentHashMap<>();

    /**
     * Build (once per execution) the chunk queue covering {@code [0, rows)}.
     *
     * @param key       the per-execution key
     * @param rows      total rows to cover
     * @param chunk     rows per chunk
     * @return the shared queue for this execution
     */
    private static ConcurrentLinkedQueue<long[]> buildQueue(String key, long rows, long chunk) {
        return QUEUES.computeIfAbsent(key, k -> {
            java.util.ArrayList<long[]> items = new java.util.ArrayList<>();
            long partitionId = 0;
            for (long start = 0; start < rows; start += chunk) {
                items.add(new long[] {partitionId++, start, Math.min(start + chunk, rows)});
            }
            return new ConcurrentLinkedQueue<>(items);
        });
    }

    private static Schema single(String name) {
        return Schemas.of(Schemas.nullable(name, Schemas.INT64));
    }

    /**
     * Per-worker cursor over the shared queue, plus the one-shot cache-control
     * advertise flag (the TTL latches on whichever worker emits first).
     */
    abstract static class ChunkState extends TableProducerState {
        /** Per-execution queue key, used to re-resolve {@link #queueRef} after a state hop. */
        public String execKey;
        /** The shared queue; transient because it is process-local. */
        public transient ConcurrentLinkedQueue<long[]> queueRef;
        /** The partition id of the chunk being emitted, or {@code -1} before the first pull. */
        public long partitionId = -1;
        /** Exclusive end of the current chunk. */
        public long currentEnd = -1;
        /** Next row index within the current chunk. */
        public long currentIdx;
        /** Whether this worker has already advertised the cache control. */
        public boolean advertised;
        /** Whether a chunk has been pulled at all. */
        public boolean haveChunk;

        ChunkState() {}

        ChunkState(ConcurrentLinkedQueue<long[]> queue, String execKey) {
            this.queueRef = queue;
            this.execKey = execKey;
        }

        final ConcurrentLinkedQueue<long[]> queue() {
            if (queueRef == null) queueRef = QUEUES.get(execKey);
            return queueRef;
        }

        /**
         * Pull the next chunk when the current one is exhausted.
         *
         * @return {@code false} when the queue is drained (the caller must finish)
         */
        final boolean advance() {
            if (haveChunk && currentIdx < currentEnd) return true;
            long[] item = queue() == null ? null : queue().poll();
            if (item == null) return false;
            partitionId = item[0];
            currentIdx = item[1];
            currentEnd = item[2];
            haveChunk = true;
            return true;
        }

        /** {@return the cache-control metadata on this worker's first batch, else {@code null}} */
        final Map<String, String> advertiseOnce() {
            if (advertised) return null;
            advertised = true;
            return CacheControl.ttl(CacheFunctions.DEFAULT_TTL_SECONDS).toMetadata();
        }
    }

    // =====================================================================
    // cache_parallel(rows, batch_size := 24000) -> v int64
    // =====================================================================

    /**
     * Multi-worker cacheable sequence — one capture substream per worker. Values
     * are the plain sequence {@code [0..rows)}, so COUNT and SUM are independent
     * of how the chunks were distributed.
     */
    public static final class CacheParallel extends SimpleTableFunction {

        private static final Schema OUTPUT = single("v");
        /** Bounded fan-out: chunk count scales with workers, not row count. */
        private static final long MAX_CHUNKS = 24;

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_parallel")
                .metadata(FunctionMetadata.describe(
                        "Multi-worker cacheable sequence (one substream per worker); parallel-capture fixture")
                        .withCategories("generator", "cache", "testing"))
                .constArg("rows", Schemas.INT64)
                .named("batch_size", Schemas.INT64, "24000")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }
        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long rows = ex.positional(0, "rows").asLong().required();
            long batchSize = ex.named("batch_size").asLong().orElse(24000L);
            long chunk = Math.max(1, (rows + MAX_CHUNKS - 1) / MAX_CHUNKS);
            String key = HexId.encode(p.executionId());
            return new State(buildQueue(key, rows, chunk), key, batchSize);
        }

        /** Per-worker chunk cursor with a caller-controlled batch width. */
        public static final class State extends ChunkState {
            /** Rows per emitted batch. */
            public long batchSize = 24000L;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(ConcurrentLinkedQueue<long[]> q, String execKey, long batchSize) {
                super(q, execKey);
                this.batchSize = batchSize;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (!advance()) { out.finish(); return; }
                int n = (int) Math.min(batchSize, currentEnd - currentIdx);
                long start = currentIdx;
                BatchUtil.emit(OUTPUT, n, out, advertiseOnce(), (root, rows, ignored) -> {
                    BigIntVector v = (BigIntVector) root.getVector("v");
                    for (int i = 0; i < rows; i++) v.setSafe(i, start + i);
                });
                currentIdx += n;
            }
        }
    }

    // =====================================================================
    // cache_ordered(rows := 200000, chunk_size := 1000) -> n int64
    // =====================================================================

    /**
     * Multi-worker, order-sensitive cacheable sequence. Capture fans out across
     * workers (the FIXED_ORDER single-thread clamp is dropped for
     * {@code supports_batch_index} functions), but a cache HIT must replay in
     * {@code batch_index} order — so tests assert row ORDER, not just the row set.
     *
     * <p>Named-with-default arguments (not positional) so it can also back the
     * catalog data table {@code example.data.cache_ordered}.
     */
    public static final class CacheOrdered extends SimpleTableFunction {

        private static final Schema OUTPUT = single("n");
        private static final long BATCH_SIZE = 256;

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_ordered")
                .metadata(FunctionMetadata.describe(
                        "Multi-worker order-sensitive cacheable sequence (batch_index); "
                        + "order-preservation cache fixture")
                        .withCategories("generator", "cache", "testing")
                        .withOrderPreservation(OrderPreservation.FIXED_ORDER)
                        .withBatchIndex())
                .named("rows", Schemas.INT64, "200000")
                .named("chunk_size", Schemas.INT64, "1000")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }
        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long rows = ex.named("rows").asLong().orElse(200000L);
            long chunk = ex.named("chunk_size").asLong().orElse(1000L);
            String key = HexId.encode(p.executionId());
            return new State(buildQueue(key, rows, chunk), key);
        }

        /** Per-worker chunk cursor emitting {@code batch_index}-tagged batches. */
        public static final class State extends ChunkState {
            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(ConcurrentLinkedQueue<long[]> q, String execKey) { super(q, execKey); }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (!advance()) { out.finish(); return; }
                int n = (int) Math.min(BATCH_SIZE, currentEnd - currentIdx);
                long start = currentIdx;
                Map<String, String> md = new LinkedHashMap<>(EmitMetadata.batchIndex(partitionId));
                Map<String, String> cc = advertiseOnce();
                if (cc != null) md.putAll(cc);
                BatchUtil.emit(OUTPUT, n, out, md, (root, rows, ignored) -> {
                    BigIntVector v = (BigIntVector) root.getVector("n");
                    for (int i = 0; i < rows; i++) v.setSafe(i, start + i);
                });
                currentIdx += n;
            }
        }
    }
}
