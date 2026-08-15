// VGI-Java example: a table function (a set-returning generator), parallel-safe.
//
// A table function produces rows. You extend `CountdownTableFunction` — a base
// for "emit rows in fixed-size batches" generators — and declare the output
// schema plus a producer. The base gives you the `count` positional arg and the
// `batch_size := 2048` named arg for free. The producer's `produceTick()` is
// called repeatedly: emit one batch per call, then call `out.finish()`.
//
// PARALLELISM. `maxWorkers()` lets DuckDB scan this function on several threads.
// Each thread gets its OWN producer, so a naive producer that counted from 0
// would re-emit the whole range once per thread. The fix: coordinate. Every
// parallel producer of one scan shares the same execution_id, hence the same
// `params.storage()` (a BoundStorage). An atomic counter there is a single cursor
// they all draw disjoint chunks from, so the union covers 0..count-1 exactly once.
//
//   ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:/abs/path/bin/runTable');
//   SELECT * FROM demo.numbers(5);                                   -- 0,1,2,3,4
//   SELECT count(*), count(DISTINCT n) FROM demo.numbers(10000000);  -- 10000000, 10000000
package farm.query.vgi.examples;

import farm.query.vgi.Worker;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.storage.BoundStorage;
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

import java.nio.charset.StandardCharsets;

/** {@code numbers(count BIGINT, batch_size := 2048) -> n BIGINT}, scanned in parallel. */
public final class TableExample extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

    // The shared cursor: a counter in a user namespace of params.storage(). All
    // parallel producers of one scan address the same (namespace, key).
    private static final byte[] CURSOR_NS = "cursor".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CURSOR_KEY = new byte[0];

    @Override public String name() { return "numbers"; }

    // Allow up to 4 parallel scan threads. Safe BECAUSE the producer coordinates
    // through storage (see produceTick); without that this would duplicate rows.
    @Override public long maxWorkers() { return 4L; }

    // Default to 2048 rows per batch — DuckDB's STANDARD_VECTOR_SIZE — so each
    // emitted batch lines up with one of the engine's vectors.
    @Override protected long defaultBatchSize() { return 2048L; }

    @Override public FunctionMetadata metadata() {
        // withPushdown(projection, filter, limit): accept LIMIT pushdown so a
        // `LIMIT 5` stops the scan early instead of materializing everything.
        return FunctionMetadata.describe("Generate the integers 0..count-1")
                .withPushdown(false, true, false)
                .withCategories("generator");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        // `count` is the positional arg; `batch_size` is the named arg the base
        // class declares (default 2048). `params.storage()` is scoped to this
        // scan's execution_id — the scope every parallel worker shares.
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().ge(1).orElse(2048L);
        return new NumbersState(count, batchSize,
                FilterApplier.from(params.pushdownFilters(), params.joinKeys()),
                params.storage());
    }

    /** Per-execution producer state. One instance per scan worker; they coordinate
     *  through the shared `storage` counter. */
    public static final class NumbersState extends TableProducerState {
        public long count;
        public long batchSize;
        public FilterApplier filters;
        public BoundStorage storage;

        public NumbersState() {}
        NumbersState(long count, long batchSize, FilterApplier filters, BoundStorage storage) {
            this.count = count; this.batchSize = batchSize; this.filters = filters; this.storage = storage;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            // Atomically reserve the next [start, start+batchSize) chunk. counterAdd
            // returns the post-add value, so concurrent calls from other workers
            // get non-overlapping chunks. When the cursor passes `count`, we're done.
            long claimedEnd = storage.counterAdd(CURSOR_NS, CURSOR_KEY, batchSize);
            long start = claimedEnd - batchSize;
            if (start >= count) { out.finish(); return; }
            int n = (int) Math.min(batchSize, count - start);

            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            BigIntVector v = (BigIntVector) root.getVector("n");
            v.allocateNew(n);
            for (int i = 0; i < n; i++) v.set(i, start + i);
            v.setValueCount(n);
            root.setRowCount(n);
            out.emit(filters.apply(root));   // emit() takes ownership of the (filtered) root
        }
    }

    public static void main(String[] args) {
        Worker.builder()
                .catalogName("demo")
                .registerTable(new TableExample())
                .runFromArgs(args);
    }
}
