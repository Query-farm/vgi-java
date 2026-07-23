// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.SimpleTableFunction;
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
import java.util.Map;

/**
 * {@code cache_filtered(rows := 100)} — cacheable sequence with static filter
 * pushdown.
 *
 * <p>The cache key folds {@code filter_bytes}, but no other cacheable fixture
 * pushes filters, so the "a pushed {@code WHERE n >= 5} must never cross-serve a
 * pushed {@code WHERE n >= 7}" boundary would otherwise be uncovered. The pushed
 * predicate is applied to each emitted batch, so distinct static filters return
 * distinct rows <em>and</em> key distinct entries.
 *
 * <p>Named-with-default {@code rows} (not positional) so it can back the catalog
 * data table {@code example.data.cache_filtered}: filter pushdown is wired on the
 * catalog scan path, which is now the only way in — {@code vgi_table_function()}
 * was removed, and every VGI function is reached through {@code ATTACH}.
 */
public final class CacheFilteredFunction extends SimpleTableFunction {

    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("n", Schemas.INT64));
    private static final int BATCH_SIZE = 2048;

    private static final FunctionSpec SPEC = FunctionSpec.builder("cache_filtered")
            .metadata(FunctionMetadata.describe(
                    "Cacheable sequence with static filter pushdown (filter_bytes keying)")
                    .withPushdown(/*projection=*/false, /*filter=*/true, /*autoApply=*/true)
                    .withCategories("generator", "cache", "testing"))
            .named("rows", Schemas.INT64, "100")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override protected Schema outputSchema() { return OUTPUT; }

    @Override public TableProducerState createProducer(TableInitParams p) {
        long rows = ParameterExtractor.of(p.arguments()).named("rows").asLong().orElse(100L);
        return new State(rows, p.pushdownFilters(), p.joinKeys());
    }

    /** Countdown state that applies the pushed predicate to each batch. */
    public static final class State extends TableProducerState {
        /** Rows still to generate (before filtering). */
        public long remaining;
        /** Index of the next row to generate. */
        public long currentIndex;
        /** Raw pushdown-filter IPC bytes, or {@code null} when none were pushed. */
        public byte[] filterBytes;
        /** Encoded join-filter keys. */
        public List<byte[]> joinKeysIpc;

        /** Required no-arg constructor for state deserialization. */
        public State() {}

        State(long remaining, byte[] filterBytes, List<byte[]> joinKeysIpc) {
            this.remaining = remaining;
            this.filterBytes = filterBytes;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) { out.finish(); return; }
            int size = (int) Math.min(remaining, BATCH_SIZE);
            long start = currentIndex;
            Map<String, String> md = currentIndex == 0
                    ? CacheControl.ttl(CacheFunctions.DEFAULT_TTL_SECONDS).toMetadata() : null;

            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
            boolean emitted = false;
            try {
                root.allocateNew();
                BigIntVector v = (BigIntVector) root.getVector("n");
                for (int i = 0; i < size; i++) v.setSafe(i, start + i);
                root.setRowCount(size);
                if (filterBytes != null) {
                    root = FilterApplier.from(filterBytes, joinKeysIpc).apply(root);
                }
                if (md == null) out.emit(root); else out.emit(root, md);
                emitted = true;
            } finally {
                if (!emitted) root.close();
            }
            currentIndex += size;
            remaining -= size;
        }
    }
}
