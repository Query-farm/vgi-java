// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Result-cache fixtures — table generators that advertise {@code vgi.cache.*} on
 * their first emitted batch so the C++ result cache can be exercised end to end.
 * Mirrors vgi-python's {@code _test_fixtures/table/cache.py}.
 *
 * <p>The multi-worker / order-sensitive members live in
 * {@link CacheParallelFunctions}; the richer-typed ones in
 * {@link CacheTypesFunction}, {@link CacheFilteredFunction} and
 * {@link CachePartitionedFunction}.
 */
public final class CacheFunctions {

    private CacheFunctions() {}

    /** Freshness lifetime for the fixtures that don't take a {@code ttl} argument. */
    static final int DEFAULT_TTL_SECONDS = 300;

    /**
     * Process-global counter advanced once per <em>real</em> invocation (in
     * {@code createProducer}, which the client only reaches on a cache MISS). A
     * served-from-cache hit never advances it, which is the value-level HIT/MISS
     * signal the tests assert on.
     */
    private static final AtomicLong NONCE = new AtomicLong();

    private static Schema single(String name) {
        return Schemas.of(Schemas.nullable(name, Schemas.INT64));
    }

    private static void fillRange(org.apache.arrow.vector.VectorSchemaRoot root, String col,
                                    long start, int rows) {
        BigIntVector v = (BigIntVector) root.getVector(col);
        for (int i = 0; i < rows; i++) v.setSafe(i, start + i);
    }

    // =====================================================================
    // cacheable_numbers(n := 10, ttl := 300) -> n int64
    // =====================================================================

    /** Baseline cacheable generator: {@code n} rows {@code [0..n)} with a TTL. */
    public static final class CacheableNumbers extends SimpleTableFunction {

        private static final Schema OUTPUT = single("n");
        private static final int BATCH_SIZE = 1000;

        private static final FunctionSpec SPEC = FunctionSpec.builder("cacheable_numbers")
                .metadata(FunctionMetadata.describe("Emits n rows [0..n) and advertises a cache TTL")
                        .withCategories("generator", "cache"))
                .named("n", Schemas.INT64, "10")
                .named("ttl", Schemas.INT64, "300")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            return new State(ex.named("n").asLong().orElse(10L),
                    (int) ex.named("ttl").asLong().orElse((long) DEFAULT_TTL_SECONDS));
        }

        /** Countdown state over the requested row range. */
        public static final class State extends TableProducerState {
            /** Rows still to emit. */
            public long remaining;
            /** Index of the next row to emit. */
            public long currentIndex;
            /** Advertised freshness lifetime in seconds. */
            public int ttl;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(long remaining, int ttl) { this.remaining = remaining; this.ttl = ttl; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (remaining <= 0) { out.finish(); return; }
                int size = (int) Math.min(remaining, BATCH_SIZE);
                long start = currentIndex;
                Map<String, String> md = currentIndex == 0 ? CacheControl.ttl(ttl).toMetadata() : null;
                BatchUtil.emit(OUTPUT, size, out, md, (root, rows, ignored) -> fillRange(root, "n", start, rows));
                currentIndex += size;
                remaining -= size;
            }
        }
    }

    // =====================================================================
    // cache_nonce() -> nonce int64
    // =====================================================================

    /** One row whose value changes on every real invocation; cacheable. */
    public static final class CacheNonce extends SimpleTableFunction {

        private static final Schema OUTPUT = single("nonce");

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_nonce")
                .metadata(FunctionMetadata.describe("Emits one row with a per-invocation nonce; cacheable")
                        .withCategories("generator", "cache", "testing"))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new State(NONCE.getAndIncrement());
        }

        /** Carries the nonce minted for this invocation. */
        public static final class State extends TableProducerState {
            /** The per-invocation nonce. */
            public long nonce;
            /** Whether the single row has been emitted. */
            public boolean done;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(long nonce) { this.nonce = nonce; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (done) { out.finish(); return; }
                done = true;
                long v = nonce;
                BatchUtil.emit(OUTPUT, 1, out, CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata(),
                        (root, rows, ignored) -> ((BigIntVector) root.getVector("nonce")).setSafe(0, v));
            }
        }
    }

    // =====================================================================
    // cache_no_store(n := 10) -> n int64
    // =====================================================================

    /** Emits rows but advertises {@code no_store}, so the client must never cache. */
    public static final class CacheNoStore extends SimpleTableFunction {

        private static final Schema OUTPUT = single("n");
        private static final int BATCH_SIZE = 1000;

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_no_store")
                .metadata(FunctionMetadata.describe("Emits n rows but advertises no_store (never cached)")
                        .withCategories("generator", "cache", "testing"))
                .named("n", Schemas.INT64, "10")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new State(ParameterExtractor.of(p.arguments()).named("n").asLong().orElse(10L));
        }

        /** Countdown state over the requested row range. */
        public static final class State extends TableProducerState {
            /** Rows still to emit. */
            public long remaining;
            /** Index of the next row to emit. */
            public long currentIndex;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(long remaining) { this.remaining = remaining; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (remaining <= 0) { out.finish(); return; }
                int size = (int) Math.min(remaining, BATCH_SIZE);
                long start = currentIndex;
                Map<String, String> md = currentIndex == 0 ? CacheControl.noStore().toMetadata() : null;
                BatchUtil.emit(OUTPUT, size, out, md, (root, rows, ignored) -> fillRange(root, "n", start, rows));
                currentIndex += size;
                remaining -= size;
            }
        }
    }

    // =====================================================================
    // cache_scoped_txn(n := 10) -> (n, nonce) int64
    // =====================================================================

    /** Advertises {@code scope=transaction}; the nonce proves same-txn HIT vs new-txn MISS. */
    public static final class CacheScopedTxn extends SimpleTableFunction {

        private static final Schema OUTPUT = Schemas.of(
                Schemas.nullable("n", Schemas.INT64),
                Schemas.nullable("nonce", Schemas.INT64));
        private static final int BATCH_SIZE = 1000;

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_scoped_txn")
                .metadata(FunctionMetadata.describe("Emits n rows and advertises scope=transaction")
                        .withCategories("generator", "cache", "testing"))
                .named("n", Schemas.INT64, "10")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new State(ParameterExtractor.of(p.arguments()).named("n").asLong().orElse(10L),
                    NONCE.getAndIncrement());
        }

        /** Countdown state plus the per-invocation nonce. */
        public static final class State extends TableProducerState {
            /** Rows still to emit. */
            public long remaining;
            /** Index of the next row to emit. */
            public long currentIndex;
            /** The per-invocation nonce, repeated on every row. */
            public long nonce;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(long remaining, long nonce) { this.remaining = remaining; this.nonce = nonce; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (remaining <= 0) { out.finish(); return; }
                int size = (int) Math.min(remaining, BATCH_SIZE);
                long start = currentIndex;
                long nv = nonce;
                Map<String, String> md = currentIndex == 0
                        ? CacheControl.builder().ttl(DEFAULT_TTL_SECONDS)
                                .scope(CacheControl.SCOPE_TRANSACTION).build().toMetadata()
                        : null;
                BatchUtil.emit(OUTPUT, size, out, md, (root, rows, ignored) -> {
                    fillRange(root, "n", start, rows);
                    BigIntVector nonceVec = (BigIntVector) root.getVector("nonce");
                    for (int i = 0; i < rows; i++) nonceVec.setSafe(i, nv);
                });
                currentIndex += size;
                remaining -= size;
            }
        }
    }

    // =====================================================================
    // cache_big(rows := 5000) -> n int64
    // =====================================================================

    /** Many small batches so multi-batch capture and replay are exercised. */
    public static final class CacheBig extends SimpleTableFunction {

        private static final Schema OUTPUT = single("n");
        private static final int BATCH_SIZE = 1000;

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_big")
                .metadata(FunctionMetadata.describe("Emits many small batches totaling `rows` rows; cacheable")
                        .withCategories("generator", "cache", "testing"))
                .named("rows", Schemas.INT64, "5000")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new State(ParameterExtractor.of(p.arguments()).named("rows").asLong().orElse(5000L));
        }

        /** Countdown state over the requested row range. */
        public static final class State extends TableProducerState {
            /** Rows still to emit. */
            public long remaining;
            /** Index of the next row to emit. */
            public long currentIndex;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(long remaining) { this.remaining = remaining; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (remaining <= 0) { out.finish(); return; }
                int size = (int) Math.min(remaining, BATCH_SIZE);
                long start = currentIndex;
                Map<String, String> md = currentIndex == 0
                        ? CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata() : null;
                BatchUtil.emit(OUTPUT, size, out, md, (root, rows, ignored) -> fillRange(root, "n", start, rows));
                currentIndex += size;
                remaining -= size;
            }
        }
    }

    // =====================================================================
    // cache_revalidatable() -> nonce int64
    // =====================================================================

    /**
     * Always-revalidate result: {@code ttl=0} + {@code etag} + {@code revalidatable}.
     * Every repeat sends a conditional request; because the data never changes the
     * worker answers with a 0-row {@code not_modified} batch and the client reuses
     * its stored payload — so the served nonce stays stable.
     */
    public static final class CacheRevalidatable extends SimpleTableFunction {

        private static final Schema OUTPUT = single("nonce");
        static final String ETAG = "\"rev-v1\"";

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_revalidatable")
                .metadata(FunctionMetadata.describe(
                        "Emits one nonce row; always-revalidate (304 not_modified)")
                        .withCategories("generator", "cache", "testing"))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new State(NONCE.getAndIncrement());
        }

        /** Carries the nonce minted for this invocation. */
        public static final class State extends TableProducerState {
            /** The per-invocation nonce. */
            public long nonce;
            /** Whether this producer has answered. */
            public boolean done;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(long nonce) { this.nonce = nonce; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                emit(null, out);
            }

            @Override public void produceTick(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                emit(input == null ? null : input.customMetadata().get(CacheControl.IF_NONE_MATCH_KEY), out);
            }

            private void emit(String ifNoneMatch, OutputCollector out) {
                if (done) { out.finish(); return; }
                done = true;
                if (ETAG.equals(ifNoneMatch)) {
                    // 304: the client's stored copy is still valid. A 0-row
                    // not_modified batch (with fresh validators + ttl=0 so it keeps
                    // revalidating) tells it to reuse the stored payload.
                    Map<String, String> md = CacheControl.builder()
                            .notModified(true).ttl(0).etag(ETAG).revalidatable(true)
                            .build().toMetadata();
                    BatchUtil.emit(OUTPUT, 0, out, md, (root, rows, ignored) -> { });
                    return;
                }
                long v = nonce;
                Map<String, String> md = CacheControl.builder()
                        .ttl(0).etag(ETAG).revalidatable(true).build().toMetadata();
                BatchUtil.emit(OUTPUT, 1, out, md,
                        (root, rows, ignored) -> ((BigIntVector) root.getVector("nonce")).setSafe(0, v));
            }
        }
    }

    // =====================================================================
    // cache_multicol(n := 4, ttl := 300) -> (a, b, c) int64
    // =====================================================================

    /** Three columns {@code (i, i*10, i*100)}; a subset projection reuses the full entry. */
    public static final class CacheMultiCol extends SimpleTableFunction {

        private static final Schema OUTPUT = Schemas.of(
                Schemas.nullable("a", Schemas.INT64),
                Schemas.nullable("b", Schemas.INT64),
                Schemas.nullable("c", Schemas.INT64));

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_multicol")
                .metadata(FunctionMetadata.describe("Emits n rows of (a, b, c); cacheable, multi-column")
                        .withCategories("generator", "cache", "testing"))
                .named("n", Schemas.INT64, "4")
                .named("ttl", Schemas.INT64, "300")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new State((int) ParameterExtractor.of(p.arguments()).named("n").asLong().orElse(4L));
        }

        /** Emits all rows in a single batch. */
        public static final class State extends TableProducerState {
            /** Rows to emit; zeroed once emitted. */
            public int remaining;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(int remaining) { this.remaining = remaining; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (remaining <= 0) { out.finish(); return; }
                int size = remaining;
                remaining = 0;
                BatchUtil.emit(OUTPUT, size, out, CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata(),
                        (root, rows, ignored) -> {
                    BigIntVector a = (BigIntVector) root.getVector("a");
                    BigIntVector b = (BigIntVector) root.getVector("b");
                    BigIntVector c = (BigIntVector) root.getVector("c");
                    for (int i = 0; i < rows; i++) {
                        a.setSafe(i, i);
                        b.setSafe(i, i * 10L);
                        c.setSafe(i, i * 100L);
                    }
                });
            }
        }
    }

    // =====================================================================
    // cache_whoami() -> who utf8
    // =====================================================================

    /**
     * Emits the caller's auth principal ({@code "anonymous"} if none); cacheable.
     * Two attaches with different bearer tokens must land under different
     * identity-scoped cache keys and never cross-serve.
     */
    public static final class CacheWhoami extends SimpleTableFunction {

        private static final Schema OUTPUT = Schemas.of(Schemas.nullable("who", Schemas.UTF8));

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_whoami")
                .metadata(FunctionMetadata.describe(
                        "Emits the caller's auth principal; cacheable (identity-scoped)")
                        .withCategories("generator", "cache", "testing"))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) { return new State(); }

        /** Reads the principal off the per-call context at emit time. */
        public static final class State extends TableProducerState {
            /** Whether the single row has been emitted. */
            public boolean done;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (done) { out.finish(); return; }
                done = true;
                String principal = ctx.auth() == null ? null : ctx.auth().principal();
                String who = (principal == null || principal.isEmpty()) ? "anonymous" : principal;
                BatchUtil.emit(OUTPUT, 1, out, CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata(),
                        (root, rows, ignored) ->
                                ((VarCharVector) root.getVector("who")).setSafe(0, new Text(who)));
            }
        }
    }

    // =====================================================================
    // cache_versioned_scan(version) -> v int64
    // =====================================================================

    /**
     * Version-specific rows; cacheable. Backs the columns-based
     * {@code example.data.cache_versioned} table, whose AT clause the catalog
     * resolves into this function's positional {@code version} argument.
     */
    public static final class CacheVersioned extends SimpleTableFunction {

        private static final Schema OUTPUT = single("v");

        /** Version to row data; the current version is {@code 3}. */
        private static final Map<Integer, long[]> DATA = Map.of(
                1, new long[] {101, 102, 103},
                2, new long[] {201, 202},
                3, new long[] {301, 302, 303, 304});
        private static final int CURRENT = 3;

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_versioned_scan")
                .metadata(FunctionMetadata.describe("Version-specific rows; cacheable (AT-keyed)")
                        .withCategories("generator", "cache", "testing"))
                .constArg("version", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            long version = ParameterExtractor.of(p.arguments()).positional(0, "version").asLong().required();
            return new State(DATA.getOrDefault((int) version, DATA.get(CURRENT)));
        }

        /** Holds the resolved version's rows. */
        public static final class State extends TableProducerState {
            /** The rows for the resolved version. */
            public long[] values;
            /** Whether the batch has been emitted. */
            public boolean done;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(long[] values) { this.values = values; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (done) { out.finish(); return; }
                done = true;
                long[] vals = values;
                BatchUtil.emit(OUTPUT, vals.length, out, CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata(),
                        (root, rows, ignored) -> {
                    BigIntVector v = (BigIntVector) root.getVector("v");
                    for (int i = 0; i < rows; i++) v.setSafe(i, vals[i]);
                });
            }
        }
    }

    // =====================================================================
    // cache_projection() -> (a, b, c) int64, projection pushdown
    // =====================================================================

    /**
     * Three-column generator that <em>pushes</em> projection, so {@code SELECT a}
     * and {@code SELECT b} push distinct {@code projection_ids} that are part of
     * the cache key — one column's bytes can never be served for another's.
     */
    public static final class CacheProjection extends SimpleTableFunction {

        private static final Schema OUTPUT = Schemas.of(
                Schemas.nullable("a", Schemas.INT64),
                Schemas.nullable("b", Schemas.INT64),
                Schemas.nullable("c", Schemas.INT64));

        private static final Map<String, long[]> DATA = Map.of(
                "a", new long[] {1, 2, 3},
                "b", new long[] {10, 20, 30},
                "c", new long[] {100, 200, 300});

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_projection")
                .metadata(FunctionMetadata.describe("3-column projection-pushdown generator; cacheable")
                        .withPushdown(/*projection=*/true, /*filter=*/false, /*autoApply=*/false)
                        .withCategories("generator", "cache", "testing"))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) { return new State(p); }

        /** Emits only the framework-narrowed projected columns. */
        public static final class State extends TableProducerState {
            /** Whether the batch has been emitted. */
            public boolean done;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(TableInitParams p) { super(p); }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (done) { out.finish(); return; }
                done = true;
                Schema projected = outputSchema;
                BatchUtil.emit(projected, 3, out, CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata(),
                        (root, rows, ignored) -> {
                    for (var field : projected.getFields()) {
                        BigIntVector v = (BigIntVector) root.getVector(field.getName());
                        long[] vals = DATA.get(field.getName());
                        for (int i = 0; i < rows; i++) v.setSafe(i, vals[i]);
                    }
                });
            }
        }
    }

    // =====================================================================
    // cache_poison() -> n int64
    // =====================================================================

    /**
     * Emits a cacheable first batch, then raises. Adversarial check of the
     * never-partial invariant: the failing thread never reaches EOS, so nothing
     * may be committed to the cache.
     */
    public static final class CachePoison extends SimpleTableFunction {

        private static final Schema OUTPUT = single("n");

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_poison")
                .metadata(FunctionMetadata.describe(
                        "Cacheable first batch then a mid-stream error (never-partial check)")
                        .withCategories("generator", "cache", "testing"))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) { return new State(); }

        /** Two-tick poison sequence: cacheable batch, then the failure. */
        public static final class State extends TableProducerState {
            /** Whether the cacheable batch has been emitted. */
            public boolean emitted;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (!emitted) {
                    emitted = true;
                    BatchUtil.emit(OUTPUT, 3, out, CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata(),
                            (root, rows, ignored) -> fillRange(root, "n", 0, rows));
                    return;
                }
                throw new IllegalStateException(
                        "cache_poison: intentional mid-stream failure after a cacheable batch");
            }
        }
    }

    // =====================================================================
    // cache_external_fail() -> n int64
    // =====================================================================

    /**
     * Emits a cacheable batch, then a 0-row pointer batch to an unreachable
     * location. The client's resolution throws mid-stream — the second
     * never-partial check.
     */
    public static final class CacheExternalFail extends SimpleTableFunction {

        private static final Schema OUTPUT = single("n");

        /** Port 9 (discard) on loopback is closed, so resolution fails fast. */
        private static final String UNRESOLVABLE_LOCATION =
                "http://127.0.0.1:9/vgi-cache-poison-nonexistent";

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_external_fail")
                .metadata(FunctionMetadata.describe(
                        "Cacheable first batch then an unresolvable external-location pointer")
                        .withCategories("generator", "cache", "testing"))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) { return new State(); }

        /** Three-tick sequence: cacheable batch, poison pointer, then finish. */
        public static final class State extends TableProducerState {
            /** Whether the cacheable batch has been emitted. */
            public boolean emitted;
            /** Whether the unresolvable pointer batch has been emitted. */
            public boolean poisoned;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (!emitted) {
                    emitted = true;
                    BatchUtil.emit(OUTPUT, 3, out, CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata(),
                            (root, rows, ignored) -> fillRange(root, "n", 0, rows));
                    return;
                }
                if (!poisoned) {
                    poisoned = true;
                    BatchUtil.emit(OUTPUT, 0, out, Map.of("vgi_rpc.location", UNRESOLVABLE_LOCATION),
                            (root, rows, ignored) -> { });
                    return;
                }
                // Only reached on a transport that doesn't resolve external
                // locations; keeps the producer from looping forever.
                out.finish();
            }
        }
    }

    // =====================================================================
    // cache_bench(rows) -> v int64
    // =====================================================================

    /**
     * Caller-controlled result size via a <em>positional</em> argument, so a
     * schema-qualified call ({@code example.cache_bench(rows)}) sizes the result.
     * Backs the scaling bench and the disk-streaming / entry-cap guards.
     */
    public static final class CacheBench extends SimpleTableFunction {

        private static final Schema OUTPUT = single("v");
        private static final int BATCH_SIZE = 2048;

        private static final FunctionSpec SPEC = FunctionSpec.builder("cache_bench")
                .metadata(FunctionMetadata.describe(
                        "Emits `rows` int64 rows (positional arg); cacheable — scaling bench fixture")
                        .withCategories("generator", "cache", "testing"))
                .constArg("rows", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new State(ParameterExtractor.of(p.arguments()).positional(0, "rows").asLong().required());
        }

        /** Countdown state over the requested row range. */
        public static final class State extends TableProducerState {
            /** Rows still to emit. */
            public long remaining;
            /** Index of the next row to emit. */
            public long currentIndex;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(long remaining) { this.remaining = remaining; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (remaining <= 0) { out.finish(); return; }
                int size = (int) Math.min(remaining, BATCH_SIZE);
                long start = currentIndex;
                Map<String, String> md = currentIndex == 0
                        ? CacheControl.ttl(DEFAULT_TTL_SECONDS).toMetadata() : null;
                BatchUtil.emit(OUTPUT, size, out, md, (root, rows, ignored) -> fillRange(root, "v", start, rows));
                currentIndex += size;
                remaining -= size;
            }
        }
    }
}
