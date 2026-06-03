// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.HexId;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code profiling_demo(count BIGINT [const], batch_size := 1024, increment := 1)}
 * — emits a sequence of {@code count} integers, but additionally publishes
 * per-execution {@code rows_produced} / {@code batches_emitted} / {@code
 * elapsed_ms} via the {@link TableFunction#dynamicToString} callback so
 * EXPLAIN ANALYZE can render them as Extra Info.
 */
public final class ProfilingDemoFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

    /** Shared per-execution counters keyed by global_execution_id (hex). */
    private static final ConcurrentHashMap<String, ExecutionStats> STATS = new ConcurrentHashMap<>();

    static final class ExecutionStats {
        final AtomicLong rows = new AtomicLong();
        final AtomicLong batches = new AtomicLong();
        final long startedAtNs = System.nanoTime();
    }

    private static String key(byte[] executionId) { return HexId.encode(executionId); }

    @Override public String name() { return "profiling_demo"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Sequence generator publishing diagnostics under EXPLAIN ANALYZE");
    }
    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override protected long defaultBatchSize() { return 1024L; }
    @Override protected List<ArgSpec> extraArgs() {
        return List.of(ArgSpec.named("increment", Schemas.INT64, "1"));
    }

    @Override public TableProducerState createProducer(TableInitParams p) {
        ParameterExtractor ex = ParameterExtractor.of(p.arguments());
        long count = ex.positional(0, "count").asLong().required();
        long batchSize = ex.named("batch_size").asLong().orElse(1024L);
        long increment = ex.named("increment").asLong().orElse(1L);
        String execKey = key(p.executionId());
        ExecutionStats stats = STATS.computeIfAbsent(execKey, k -> new ExecutionStats());
        return new State(new BatchState(count, batchSize), increment, execKey, stats);
    }

    @Override
    public java.util.LinkedHashMap<String, String> dynamicToString(byte[] globalExecutionId) {
        ExecutionStats s = STATS.get(key(globalExecutionId));
        java.util.LinkedHashMap<String, String> out = new LinkedHashMap<>();
        if (s == null) return out;
        long elapsedMs = (System.nanoTime() - s.startedAtNs) / 1_000_000L;
        out.put("rows_produced", Long.toString(s.rows.get()));
        out.put("batches_emitted", Long.toString(s.batches.get()));
        out.put("elapsed_ms", Long.toString(elapsedMs));
        return out;
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public BatchState batch;
        public long increment;
        public String execKey;
        public transient ExecutionStats statsRef;

        public State() {}
        State(BatchState batch, long increment, String execKey, ExecutionStats stats) {
            this.batch = batch;
            this.increment = increment;
            this.execKey = execKey;
            this.statsRef = stats;
        }

        private ExecutionStats stats() {
            if (statsRef == null) statsRef = STATS.computeIfAbsent(execKey, k -> new ExecutionStats());
            return statsRef;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            for (int i = 0; i < n; i++) v.setSafe(i, (start + i) * increment);
            root.setRowCount(n);
            out.emit(root);
            batch.advance(n);
            ExecutionStats s = stats();
            s.rows.addAndGet(n);
            s.batches.incrementAndGet();
        }
    }
}
