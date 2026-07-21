// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionMetadata.PartitionKind;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.EmitMetadata;
import farm.query.vgi.internal.HexId;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Per-<em>partition</em> result-cache fixtures — {@code SINGLE_VALUE_PARTITIONS}
 * functions that advertise {@code vgi.cache.partition_scope}, so the client ALSO
 * stores the result split by partition value and a later {@code =}/{@code IN}
 * scan on the partition column(s) serves those partitions without the worker.
 *
 * <p>All four opt into filter pushdown with auto-apply: a {@code WHERE country = …}
 * predicate reaches the worker as a real filter (so the client can enumerate the
 * requested set) <em>and</em> the fixture prunes each emitted batch to it, because
 * DuckDB does not re-apply a pushed predicate above the scan — a fall-through
 * worker scan must therefore be row-exact on its own.
 *
 * <p>{@code partition_scope} is advertised on <em>every</em> batch (not just the
 * first) so the opt-in still latches on a fall-through scan whose leading
 * partition was filtered away to zero rows.
 *
 * <p>Mirrors vgi-python's {@code vgi/_test_fixtures/table/cache.py}; backs
 * {@code cache/partition_scope{,_ops,_shapes}.test}.
 */
public final class CachePartitionScopeFunctions {

    private CachePartitionScopeFunctions() {}

    /** Countries for {@code cache_partition_scope}. */
    private static final String[] SCOPE_COUNTRIES = {"AU", "BR", "CA", "FR", "US"};
    /** Countries for {@code cache_partition_parallel}; the trailing NULL is deliberate. */
    private static final String[] PARALLEL_COUNTRIES = {"AU", "CA", "US", null};
    /** Countries for {@code cache_partition_proj}. */
    private static final String[] PROJ_COUNTRIES = {"CA", "US"};
    /**
     * {@code (region, year)} partitions for {@code cache_partition_multicol}. The
     * years are NON-contiguous on purpose: DuckDB rewrites an {@code IN} over
     * contiguous integers into a BETWEEN range (which is not enumerable), so a gap
     * keeps the pushed filter an IN filter and the cross-product enumeration path
     * is genuinely exercised.
     */
    private static final String[] REGIONS = {"EU", "US"};
    private static final long[] YEARS = {2020L, 2022L};

    /** Per-execution work queue of country indices for the parallel fixture. */
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<Integer>> QUEUES =
            new ConcurrentHashMap<>();

    private static ConcurrentLinkedQueue<Integer> buildQueue(String key, int count) {
        return QUEUES.computeIfAbsent(key, k -> {
            List<Integer> items = new ArrayList<>();
            for (int i = 0; i < count; i++) items.add(i);
            return new ConcurrentLinkedQueue<>(items);
        });
    }

    /** {@return the {@code ttl} + {@code partition_scope} advertisement carried by every batch} */
    private static Map<String, String> advertise() {
        return CacheControl.builder()
                .ttl(CacheFunctions.DEFAULT_TTL_SECONDS)
                .partitionScope(true)
                .build()
                .toMetadata();
    }

    /**
     * Fill {@code country}/{@code sales} for one single-valued partition, apply any
     * pushed filter, then emit with the partition-scope advertisement.
     *
     * @param schema   the emitted schema
     * @param country  the partition value ({@code null} emits a NULL partition)
     * @param base     first {@code sales} value
     * @param rows     row count before filtering
     * @param filters  the pushed-filter applier, or {@code null}
     * @param explicit explicit partition values, or {@code null} to auto-extract
     * @param declared the declared (unprojected) schema partition columns come from
     * @param out      the collector
     */
    private static void emitCountryBatch(Schema schema, String country, long base, int rows,
                                         FilterApplier filters,
                                         Map<String, EmitMetadata.Range> explicit,
                                         Schema declared, OutputCollector out) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        boolean emitted = false;
        try {
            root.allocateNew();
            for (org.apache.arrow.vector.types.pojo.Field f : schema.getFields()) {
                FieldVector v = root.getVector(f.getName());
                switch (f.getName()) {
                    case "country" -> {
                        VarCharVector cv = (VarCharVector) v;
                        if (country == null) {
                            for (int i = 0; i < rows; i++) cv.setNull(i);
                        } else {
                            Text t = new Text(country);
                            for (int i = 0; i < rows; i++) cv.setSafe(i, t);
                        }
                    }
                    case "sales" -> {
                        BigIntVector sv = (BigIntVector) v;
                        for (int i = 0; i < rows; i++) sv.setSafe(i, base + i);
                    }
                    case "extra" -> {
                        BigIntVector ev = (BigIntVector) v;
                        for (int i = 0; i < rows; i++) ev.setSafe(i, base + 500 + i);
                    }
                    default -> { }
                }
            }
            root.setRowCount(rows);
            if (filters != null) root = filters.apply(root);

            Map<String, String> md = new LinkedHashMap<>();
            Map<String, String> pv = EmitMetadata.partitionValues(declared, root, explicit);
            if (pv != null) md.putAll(pv);
            md.putAll(advertise());
            out.emit(root, md);
            emitted = true;
        } finally {
            if (!emitted) root.close();
        }
    }

    // =====================================================================
    // cache_partition_scope(rows_per_country)
    // =====================================================================

    /**
     * Per-partition cacheable single-value-partitioned result ({@code country},
     * {@code sales}); one batch per country, single worker.
     */
    public static final class CachePartitionScope extends SimpleTableFunction {

        private static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("country", Schemas.UTF8),
                Schemas.nullable("sales", Schemas.INT64));

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_partition_scope")
                .metadata(FunctionMetadata.describe(
                        "Per-partition cacheable single-value-partitioned result "
                        + "(vgi.cache.partition_scope)")
                        .withCategories("generator", "cache", "testing", "partitioning")
                        .withPushdown(/*projection=*/false, /*filter=*/true, /*autoApply=*/true)
                        .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS))
                .constArg("rows_per_country", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            State s = new State();
            s.rowsPerCountry = (int) ParameterExtractor.of(p.arguments())
                    .positional(0, "rows_per_country").asLong().required();
            s.filterBytes = p.pushdownFilters();
            s.joinKeysIpc = p.joinKeys();
            return s;
        }

        /** Cursor over the fixed country list. */
        public static final class State extends TableProducerState {
            /** Index of the next country to emit. */
            public int countryIdx;
            /** Rows emitted per country, before filtering. */
            public int rowsPerCountry;
            /** Raw pushdown-filter IPC bytes, or {@code null} when none were pushed. */
            public byte[] filterBytes;
            /** Encoded join-filter keys. */
            public List<byte[]> joinKeysIpc;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (countryIdx >= SCOPE_COUNTRIES.length) { out.finish(); return; }
                String country = SCOPE_COUNTRIES[countryIdx];
                long base = (long) countryIdx * 1_000_000L;
                countryIdx++;
                emitCountryBatch(OUTPUT, country, base, rowsPerCountry,
                        filterBytes == null ? null : FilterApplier.from(filterBytes, joinKeysIpc),
                        null, OUTPUT, out);
            }
        }
    }

    // =====================================================================
    // cache_partition_parallel(rows_per_country)
    // =====================================================================

    /**
     * Per-partition cacheable with work-queue fan-out, so a {@code threads=N} scan
     * spreads partitions across workers and the per-partition split at commit must
     * bucket batches drawn from multiple capture substreams. One partition is NULL
     * (SINGLE_VALUE permits NULL), covering capture/serve of a NULL tuple and the
     * correct non-enumerability of {@code IS NULL}.
     */
    public static final class CachePartitionParallel extends SimpleTableFunction {

        private static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("country", Schemas.UTF8),
                Schemas.nullable("sales", Schemas.INT64));

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_partition_parallel")
                .metadata(FunctionMetadata.describe(
                        "Per-partition cacheable; work-queue fan-out (parallel capture); "
                        + "one NULL partition")
                        .withCategories("generator", "cache", "testing", "partitioning")
                        .withPushdown(/*projection=*/false, /*filter=*/true, /*autoApply=*/true)
                        .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS))
                .constArg("rows_per_country", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }
        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            State s = new State();
            s.rowsPerCountry = (int) ParameterExtractor.of(p.arguments())
                    .positional(0, "rows_per_country").asLong().required();
            s.execKey = HexId.encode(p.executionId());
            s.queueRef = buildQueue(s.execKey, PARALLEL_COUNTRIES.length);
            s.filterBytes = p.pushdownFilters();
            s.joinKeysIpc = p.joinKeys();
            return s;
        }

        /** Per-worker cursor over the shared queue of country indices. */
        public static final class State extends TableProducerState {
            /** Per-execution queue key, used to re-resolve {@link #queueRef} after a state hop. */
            public String execKey;
            /** The shared queue; transient because it is process-local. */
            public transient ConcurrentLinkedQueue<Integer> queueRef;
            /** Rows emitted per country, before filtering. */
            public int rowsPerCountry;
            /** Raw pushdown-filter IPC bytes, or {@code null} when none were pushed. */
            public byte[] filterBytes;
            /** Encoded join-filter keys. */
            public List<byte[]> joinKeysIpc;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (queueRef == null) queueRef = QUEUES.get(execKey);
                Integer idx = queueRef == null ? null : queueRef.poll();
                if (idx == null) { out.finish(); return; }
                String country = PARALLEL_COUNTRIES[idx];
                long base = (long) idx * 1_000_000L;
                // Explicit partition values keep the NULL partition unambiguous
                // (an all-NULL column auto-extracts to a NULL range too, but the
                // explicit form pins the scalar's type).
                Map<String, EmitMetadata.Range> explicit =
                        Map.of("country", new EmitMetadata.Range(country, country));
                emitCountryBatch(OUTPUT, country, base, rowsPerCountry,
                        filterBytes == null ? null : FilterApplier.from(filterBytes, joinKeysIpc),
                        explicit, OUTPUT, out);
            }
        }
    }

    // =====================================================================
    // cache_partition_multicol(rows_per_partition)
    // =====================================================================

    /**
     * Per-partition cacheable over TWO single-value partition columns
     * ({@code region}, {@code year}) — exercises cross-product enumeration
     * ({@code region IN} × {@code year IN}), two-column tuple canonicalization, and
     * the partially-constrained case (region constrained, year free → not
     * enumerable → falls through to the worker).
     */
    public static final class CachePartitionMultiCol extends SimpleTableFunction {

        private static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("region", Schemas.UTF8),
                EmitMetadata.partitionField("year", Schemas.INT64),
                Schemas.nullable("amount", Schemas.INT64));

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_partition_multicol")
                .metadata(FunctionMetadata.describe(
                        "Per-partition cacheable over (region, year) SINGLE_VALUE partition columns")
                        .withCategories("generator", "cache", "testing", "partitioning")
                        .withPushdown(/*projection=*/false, /*filter=*/true, /*autoApply=*/true)
                        .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS))
                .constArg("rows_per_partition", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            State s = new State();
            s.rowsPerPartition = (int) ParameterExtractor.of(p.arguments())
                    .positional(0, "rows_per_partition").asLong().required();
            s.filterBytes = p.pushdownFilters();
            s.joinKeysIpc = p.joinKeys();
            return s;
        }

        /** Cursor over the fixed {@code (region, year)} cross product. */
        public static final class State extends TableProducerState {
            /** Index into the {@code (region, year)} cross product. */
            public int idx;
            /** Rows emitted per partition, before filtering. */
            public int rowsPerPartition;
            /** Raw pushdown-filter IPC bytes, or {@code null} when none were pushed. */
            public byte[] filterBytes;
            /** Encoded join-filter keys. */
            public List<byte[]> joinKeysIpc;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                int total = REGIONS.length * YEARS.length;
                if (idx >= total) { out.finish(); return; }
                String region = REGIONS[idx / YEARS.length];
                long year = YEARS[idx % YEARS.length];
                long base = (long) idx * 1000L;
                int rows = rowsPerPartition;
                idx++;

                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                boolean emitted = false;
                try {
                    root.allocateNew();
                    VarCharVector rv = (VarCharVector) root.getVector("region");
                    BigIntVector yv = (BigIntVector) root.getVector("year");
                    BigIntVector av = (BigIntVector) root.getVector("amount");
                    Text regionText = new Text(region);
                    for (int i = 0; i < rows; i++) {
                        rv.setSafe(i, regionText);
                        yv.setSafe(i, year);
                        av.setSafe(i, base + i);
                    }
                    root.setRowCount(rows);
                    if (filterBytes != null) {
                        root = FilterApplier.from(filterBytes, joinKeysIpc).apply(root);
                    }
                    Map<String, String> md = new LinkedHashMap<>();
                    Map<String, String> pv = EmitMetadata.partitionValues(OUTPUT, root, null);
                    if (pv != null) md.putAll(pv);
                    md.putAll(advertise());
                    out.emit(root, md);
                    emitted = true;
                } finally {
                    if (!emitted) root.close();
                }
            }
        }
    }

    // =====================================================================
    // cache_partition_proj(rows_per_country)
    // =====================================================================

    /**
     * Per-partition cacheable with projection pushdown: projection is part of the
     * cache key, and the explicit partition values mean the partition value
     * survives even when {@code country} is projected OUT of the emitted batch
     * (the auto-extract-impossible case). {@code extra} is a non-partition column
     * to project away while keeping {@code country} pushable.
     */
    public static final class CachePartitionProj extends SimpleTableFunction {

        private static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("country", Schemas.UTF8),
                Schemas.nullable("sales", Schemas.INT64),
                Schemas.nullable("extra", Schemas.INT64));

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_partition_proj")
                .metadata(FunctionMetadata.describe(
                        "Per-partition cacheable with projection pushdown + explicit "
                        + "partition_values")
                        .withCategories("generator", "cache", "testing", "partitioning")
                        .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                        .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS))
                .constArg("rows_per_country", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            State s = new State();
            s.rowsPerCountry = (int) ParameterExtractor.of(p.arguments())
                    .positional(0, "rows_per_country").asLong().required();
            s.projected = new CachedSchema(p.outputSchema());
            s.filterBytes = p.pushdownFilters();
            s.joinKeysIpc = p.joinKeys();
            return s;
        }

        /** Cursor over the fixed country list, emitting only the projected columns. */
        public static final class State extends TableProducerState {
            /** Index of the next country to emit. */
            public int countryIdx;
            /** Rows emitted per country, before filtering. */
            public int rowsPerCountry;
            /** The projected emit schema handed down by the framework. */
            public CachedSchema projected;
            /** Raw pushdown-filter IPC bytes, or {@code null} when none were pushed. */
            public byte[] filterBytes;
            /** Encoded join-filter keys. */
            public List<byte[]> joinKeysIpc;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (countryIdx >= PROJ_COUNTRIES.length) { out.finish(); return; }
                String country = PROJ_COUNTRIES[countryIdx];
                long base = (long) countryIdx * 1_000_000L;
                countryIdx++;
                // Explicit pv: `country` may have been projected away, so it cannot
                // always be auto-extracted from the emitted batch.
                Map<String, EmitMetadata.Range> explicit =
                        Map.of("country", new EmitMetadata.Range(country, country));
                emitCountryBatch(projected.get(), country, base, rowsPerCountry,
                        filterBytes == null ? null : FilterApplier.from(filterBytes, joinKeysIpc),
                        explicit, OUTPUT, out);
            }
        }
    }
}
