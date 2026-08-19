// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.EmitMetadata;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.pushdown.PushdownFilter;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.table.PlanRequest;
import farm.query.vgi.table.PlanResult;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Split-capable table generators, the Java half of the cross-SDK splits suite.
 *
 * <p>Every fixture here is a TWIN of one in vgi-python of the same name. The
 * shared SQL suite runs unchanged against every SDK's worker, so a wire
 * disagreement between two SDKs shows up as the same named test failing under
 * one of them — which only works if the fixtures agree on BEHAVIOUR, not merely
 * on name.</p>
 *
 * <p>The shapes cover the ways a split scan goes wrong rather than the ways it
 * goes right: zero splits (legal, must be an empty result), zero-ROW splits (the
 * likelier shape — a filter pruned one — and the one that silently truncates a
 * scan if a reader treats an empty split as EOS), skew, and far more splits than
 * reader threads (which forces sequential re-init on a reused connection).</p>
 */
public final class SplitFunctions {

    private SplitFunctions() {}

    static final Schema SEQ_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

    /** The half-open range {@code [lo, hi)} one split owns. */
    record Range(long lo, long hi) {}

    /**
     * A split NAMES the work rather than describing it: a redemption reads the
     * same rows however many times it runs and whichever process runs it, which
     * is exactly what a retrying engine requires.
     */
    static byte[] encode(Range r) {
        return ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(r.lo()).putLong(r.hi()).array();
    }

    static Range decode(byte[] payload) {
        if (payload.length != 16) {
            throw new IllegalArgumentException(
                    "split payload must be 16 bytes, got " + payload.length);
        }
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        return new Range(b.getLong(), b.getLong());
    }

    /** {@code (ordinal, lo, hi)} — the batch-index fixture needs the position too. */
    static byte[] encodeOrdinal(long ordinal, Range r) {
        return ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(ordinal).putLong(r.lo()).putLong(r.hi()).array();
    }

    static long[] decodeOrdinal(byte[] payload) {
        if (payload.length != 24) {
            throw new IllegalArgumentException(
                    "batch-index split payload must be 24 bytes, got " + payload.length);
        }
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        return new long[] {b.getLong(), b.getLong(), b.getLong()};
    }

    /** Divide {@code [0, n)} into {@code k} contiguous ranges, remainder first. */
    static List<Range> evenRanges(long n, long k) {
        List<Range> out = new ArrayList<>();
        if (k <= 0) return out;
        long total = Math.max(0, n);
        long base = total / k;
        long extra = total % k;
        long lo = 0;
        for (long i = 0; i < k; i++) {
            long hi = lo + base + (i < extra ? 1 : 0);
            out.add(new Range(lo, hi));
            lo = hi;
        }
        return out;
    }

    static long argLong(TableBindParams params, String name, long fallback) {
        Object v = params.arguments().named().get(name);
        return v instanceof Number num ? num.longValue() : fallback;
    }

    static long argLong(TableInitParams params, String name, long fallback) {
        Object v = params.arguments().named().get(name);
        return v instanceof Number num ? num.longValue() : fallback;
    }

    /**
     * Read this reader's claimed ranges, or fail loudly.
     *
     * <p>No payloads at all means the client stopped planning
     * ({@code vgi_split_scans} off). A split-only function has no way to know
     * what to read then, and failing here is the point: quietly returning zero
     * rows would be A DIFFERENT ANSWER to the same query, which is worse than an
     * error. Distinct from a plan that legitimately produced ZERO splits — there
     * the client never inits at all.</p>
     */
    static List<byte[]> claimed(String name, TableInitParams params) {
        List<byte[]> payloads = params.splitPayloads();
        if (payloads == null) {
            throw new IllegalStateException(name
                    + " is split-only but was initialized with no split tokens; "
                    + "vgi_split_scans is probably off, and this function has no "
                    + "primary/secondary path to fall back to");
        }
        return payloads;
    }

    /**
     * The CANONICAL cross-SDK rendering of a pushed-down filter set.
     *
     * <p>Every SDK must produce this byte-for-byte, because the shared SQL suite
     * asserts on the string. A language's own debug formatting cannot be used —
     * Python's {@code repr(PushdownFilters)} is Python-shaped and no other SDK
     * can reproduce it, so a test asserting it could only ever pass against that
     * one worker, which defeats the point of a shared suite.</p>
     *
     * <p>For each filtered column in sorted order: {@code col>=min} and/or
     * {@code col<=max}, joined by {@code ,}. Bounds are normalized to INCLUSIVE
     * integers, because that is the only form every SDK can produce (Rust's
     * ColumnBounds carries no inclusive flag). Values are included deliberately:
     * without them a tightening Top-N filter and a loose one render identically
     * and the test cannot tell them apart.</p>
     *
     * @param pf the filters this call received, or {@code null}
     * @return the canonical rendering, or {@code "(none)"}
     */
    static String renderFiltersCanonical(PushdownFilters pf) {
        if (pf == null || pf.filters().isEmpty()) return "(none)";
        Map<String, long[]> bounds = new java.util.TreeMap<>();
        // [min, max, hasMin, hasMax]
        for (PushdownFilter f : pf.filters()) {
            collectBounds(f, bounds);
        }
        List<String> parts = new ArrayList<>();
        for (Map.Entry<String, long[]> e : bounds.entrySet()) {
            long[] b = e.getValue();
            if (b[2] != 0) parts.add(e.getKey() + ">=" + b[0]);
            if (b[3] != 0) parts.add(e.getKey() + "<=" + b[1]);
        }
        return parts.isEmpty() ? "(none)" : String.join(",", parts);
    }

    private static void note(Map<String, long[]> bounds, String col, boolean isMin, long v) {
        long[] b = bounds.computeIfAbsent(col, k -> new long[] {0, 0, 0, 0});
        if (isMin) {
            // Widest-wins, matching the other SDKs: a looser bound is always
            // sound, and agreeing on the rule matters more than tightness here.
            if (b[2] == 0 || v < b[0]) b[0] = v;
            b[2] = 1;
        } else {
            if (b[3] == 0 || v > b[1]) b[1] = v;
            b[3] = 1;
        }
    }

    private static void collectBounds(PushdownFilter f, Map<String, long[]> bounds) {
        // Recursive: a compound predicate arrives as And([Constant, Constant]),
        // so walking only the top level renders "(none)" for exactly the
        // multi-clause filters worth asserting on.
        if (f instanceof PushdownFilter.And and) {
            for (PushdownFilter c : and.children()) collectBounds(c, bounds);
        } else if (f instanceof PushdownFilter.Or or) {
            for (PushdownFilter c : or.children()) collectBounds(c, bounds);
        } else if (f instanceof PushdownFilter.Constant c && c.value() instanceof Number num) {
            long v = num.longValue();
            // Exclusive comparisons are tightened by one: bounds are integer
            // here, so `< v` is exactly `<= v - 1` and it is lossless.
            switch (c.op()) {
                case GE -> note(bounds, c.columnName(), true, v);
                case GT -> note(bounds, c.columnName(), true, v + 1);
                case LE -> note(bounds, c.columnName(), false, v);
                case LT -> note(bounds, c.columnName(), false, v - 1);
                case EQ -> {
                    note(bounds, c.columnName(), true, v);
                    note(bounds, c.columnName(), false, v);
                }
                default -> { }
            }
        } else if (f instanceof PushdownFilter.In in) {
            // An IN set implies bounds — [min(values), max(values)] — and a
            // join-key filter IS an IN set once its side batch is resolved.
            // Skipping them means a worker pruning by range gets NOTHING from a
            // join-key pushdown, the most valuable pushdown a scan receives.
            Long lo = null;
            Long hi = null;
            for (Object o : in.values()) {
                if (!(o instanceof Number num)) continue;
                long v = num.longValue();
                if (lo == null || v < lo) lo = v;
                if (hi == null || v > hi) hi = v;
            }
            if (lo != null) note(bounds, in.columnName(), true, lo);
            if (hi != null) note(bounds, in.columnName(), false, hi);
        }
    }

    /** The two named arguments the shared SQL suite binds by name across SDKs. */
    static List<ArgSpec> splitArgs() {
        return List.of(
                ArgSpec.named("n", Schemas.INT64, "0"),
                ArgSpec.named("splits", Schemas.INT64, "4"));
    }

    // =====================================================================
    // The sequence-shaped fixtures. Only the range DIVISION differs between
    // most of them, so plan/redeem/emit is shared and each supplies its shape.
    // =====================================================================

    /** Shared plan/redeem/emit machinery for the sequence-shaped split fixtures. */
    public abstract static class SplitScan implements TableFunction {

        /** How this fixture divides the scan.
         *
         * @param n rows requested
         * @param splits splits requested
         * @return the ranges, one per split
         */
        protected abstract List<Range> planRanges(long n, long splits);

        /** Batches emitted per tick. */
        protected int maxBatch() { return 1024; }

        /** Cache TTL to advertise on this reader's first batch, or {@code null}. */
        protected Integer cacheTtl() { return null; }

        /** Index-space stride per split, or 0 for no batch-index tagging. */
        protected long batchStride() { return 0; }

        /** Pin the plan to a version the live catalog will not agree with. */
        protected Long catalogVersion() { return null; }

        /** Splits per enumeration page, or 0 to emit them all at once. */
        protected int perPage() { return 0; }

        /** Split-token lifetime, or {@code null} for unbounded. */
        protected Long splitTokenTtl() { return null; }

        /** @return this fixture's registered name. */
        @Override public abstract String name();

        /** @return this fixture's one-line description. */
        protected abstract String description();

        @Override public FunctionMetadata metadata() {
            // The declaration is what a distributed engine reads to decide it
            // can retry a task against this function — and what makes the client
            // call plan() at all. Without it the scan is silently never divided.
            FunctionMetadata m = FunctionMetadata.describe(description()).withSplits();
            if (batchStride() > 0) m = m.withBatchIndex();
            if (splitTokenTtl() != null) m = m.withSplitTokenTtl(splitTokenTtl());
            return m;
        }

        // position -1 = a NAMED argument, so the shared SQL suite's
        // `split_sequence(n := 10, splits := 4)` binds identically across SDKs.
        @Override public List<ArgSpec> argumentSpecs() { return splitArgs(); }

        @Override public farm.query.vgi.protocol.BindResponse onBind(TableBindParams params) {
            return farm.query.vgi.protocol.BindResponse.forSchema(
                    farm.query.vgi.internal.SchemaUtil.serializeSchema(SEQ_SCHEMA));
        }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            long n = argLong(params, "n", 0);
            List<Range> all = planRanges(n, argLong(params, "splits", 4));

            int base = 0;
            List<Range> window = all;
            byte[] cursor = null;
            if (perPage() > 0) {
                // Pagination: hand out one window per call, cursoring on the page
                // index. The range list is regenerable from the bind arguments
                // alone, so the cursor needs to carry nothing else.
                byte[] c = request.cursor();
                int page = (c != null && c.length == 8)
                        ? (int) ByteBuffer.wrap(c).order(ByteOrder.LITTLE_ENDIAN).getLong() : 0;
                base = page * perPage();
                int lo = Math.min(base, all.size());
                int hi = Math.min(lo + perPage(), all.size());
                window = all.subList(lo, hi);
                if (base + perPage() < all.size()) {
                    cursor = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                            .putLong(page + 1).array();
                }
            }

            List<ScanSplit> splits = new ArrayList<>(window.size());
            for (int i = 0; i < window.size(); i++) {
                Range r = window.get(i);
                byte[] payload = batchStride() > 0
                        ? encodeOrdinal(base + i, r) : encode(r);
                splits.add(new ScanSplit(payload, new byte[0], r.hi() - r.lo(), true,
                        (r.hi() - r.lo()) * 8, null, null, null, null, null));
            }
            PlanResult result = PlanResult.of(splits);
            if (catalogVersion() != null) result = result.withCatalogVersion(catalogVersion());
            if (cursor != null) result = result.withNextCursor(cursor);
            return result;
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            List<byte[]> payloads = claimed(name(), params);
            List<Range> ranges = new ArrayList<>(payloads.size());
            List<Long> ordinals = new ArrayList<>(payloads.size());
            for (byte[] p : payloads) {
                if (batchStride() > 0) {
                    long[] o = decodeOrdinal(p);
                    ordinals.add(o[0]);
                    ranges.add(new Range(o[1], o[2]));
                } else {
                    ranges.add(decode(p));
                }
            }
            return new SplitState(params, ranges, ordinals, maxBatch(), cacheTtl(), batchStride());
        }
    }

    /** Walks THIS reader's claimed ranges in order, one batch per tick. */
    public static final class SplitState extends TableProducerState {
        /** The ranges this reader claimed. */
        public List<Range> ranges = List.of();
        /** Split ordinals, parallel to {@link #ranges}; empty unless batch-indexed. */
        public List<Long> ordinals = List.of();
        /** Index of the range being emitted. */
        public int idx;
        /** Next row to emit within the current range. */
        public long cur;
        /** Rows per emitted batch. */
        public int maxBatch = 1024;
        /** Cache TTL to advertise on the first batch, or {@code null}. */
        public Integer cacheTtl;
        /** Index-space stride per split, or 0. */
        public long batchStride;
        /** Batches emitted within the current split. */
        public long emittedInSplit;
        /** Set once this reader has advertised freshness. */
        public boolean cacheAdvertised;

        /** Required no-arg constructor for state deserialization. */
        public SplitState() {}

        SplitState(TableInitParams params, List<Range> ranges, List<Long> ordinals,
                int maxBatch, Integer cacheTtl, long batchStride) {
            super(params);
            this.ranges = ranges;
            this.ordinals = ordinals;
            this.maxBatch = maxBatch;
            this.cacheTtl = cacheTtl;
            this.batchStride = batchStride;
            this.cur = ranges.isEmpty() ? 0 : ranges.get(0).lo();
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            while (true) {
                if (idx >= ranges.size()) { out.finish(); return; }
                Range r = ranges.get(idx);
                if (cur >= r.hi()) {
                    // A zero-row range is STEPPED OVER, never reported as
                    // end-of-stream: finishing here would truncate this reader's
                    // remaining claims and the query would look correct while
                    // missing rows.
                    idx++;
                    emittedInSplit = 0;
                    if (idx < ranges.size()) cur = ranges.get(idx).lo();
                    continue;
                }
                int size = (int) Math.min(r.hi() - cur, maxBatch);
                long start = cur;
                cur += size;

                Map<String, String> md = null;
                if (cacheTtl != null && !cacheAdvertised) {
                    // The FIRST batch of this reader's stream — the only one the
                    // client reads freshness from. Every reader advertises the
                    // same value, because the result is one entry with one
                    // lifetime and a per-split TTL would be decided by whichever
                    // reader happened to arrive first.
                    cacheAdvertised = true;
                    md = CacheControl.ttl(cacheTtl).toMetadata();
                }
                if (batchStride > 0) {
                    long ordinal = idx < ordinals.size() ? ordinals.get(idx) : idx;
                    Map<String, String> bi =
                            EmitMetadata.batchIndex(ordinal * batchStride + emittedInSplit);
                    emittedInSplit++;
                    if (md == null) {
                        md = bi;
                    } else {
                        md = new java.util.HashMap<>(md);
                        md.putAll(bi);
                    }
                }
                BatchUtil.emit(outputSchema == null ? SEQ_SCHEMA : outputSchema, size, out, md,
                        (root, rows, ignored) -> {
                            BigIntVector v = (BigIntVector) root.getVector("n");
                            v.allocateNew(rows);
                            for (int i = 0; i < rows; i++) v.set(i, start + i);
                        });
                return;
            }
        }
    }

    // =====================================================================
    // The fixtures
    // =====================================================================

    /** The parity twin: {@code split_sequence(n)} must equal {@code sequence(n)} row for row. */
    public static final class SplitSequence extends SplitScan {
        @Override public String name() { return "split_sequence"; }
        @Override protected String description() {
            return "Split-capable twin of sequence(n): 0..n-1 divided into `splits` ranges";
        }
        @Override protected List<Range> planRanges(long n, long splits) {
            return evenRanges(n, splits);
        }
    }

    /** Returns NO splits. Legal, and it must produce an empty result rather than
     *  a crash — a fully-pruned scan reaches exactly this. */
    public static final class SplitZero extends SplitScan {
        @Override public String name() { return "split_zero"; }
        @Override protected String description() {
            return "Returns zero splits: a legal empty result, not an error";
        }
        @Override protected List<Range> planRanges(long n, long splits) { return List.of(); }
    }

    /** Interleaves EMPTY splits between non-empty ones. This is the shape that
     *  silently truncates a scan if a reader mistakes an empty split for
     *  end-of-stream, and it is far likelier in practice than zero splits. */
    public static final class SplitEmptyRanges extends SplitScan {
        @Override public String name() { return "split_empty_ranges"; }
        @Override protected String description() {
            return "Some splits yield zero rows; the scan must not end early";
        }
        @Override protected List<Range> planRanges(long n, long splits) {
            List<Range> out = new ArrayList<>();
            for (Range r : evenRanges(n, splits)) {
                out.add(new Range(r.lo(), r.lo()));
                out.add(r);
            }
            return out;
        }
    }

    /** One split holds ~99% of the rows, so greedy per-split claiming is
     *  distinguishable from static assignment: under greedy claiming the fast
     *  readers keep working while one reader owns the big split. The row count is
     *  identical either way, so this is about MAKESPAN, not correctness. */
    public static final class SplitSkewed extends SplitScan {
        @Override public String name() { return "split_skewed"; }
        @Override protected String description() {
            return "One split ~100x the others: exercises greedy claiming under skew";
        }
        @Override protected List<Range> planRanges(long n, long splits) {
            if (n <= 0 || splits <= 0) return List.of();
            long head = n * 99 / 100;
            List<Range> out = new ArrayList<>();
            out.add(new Range(0, head));
            for (Range r : evenRanges(n - head, splits - 1)) {
                out.add(new Range(head + r.lo(), head + r.hi()));
            }
            return out;
        }
    }

    /** Far more splits than reader threads, which forces sequential re-init on a
     *  REUSED connection — the path where a split-init failure would otherwise
     *  pool a connection with an unanswered init in flight. */
    public static final class SplitMany extends SplitScan {
        @Override public String name() { return "split_many"; }
        @Override protected String description() {
            return "Far more splits than threads: exercises greedy claiming and re-init";
        }
        @Override protected List<Range> planRanges(long n, long splits) {
            return evenRanges(n, splits <= 0 ? 1000 : splits);
        }
    }

    /** Enumerates its plan over several pages, each disjoint from the last.
     *
     *  <p>Disjointness is the worker's obligation and no client checks it — a
     *  dedup was tried and removed, because it needed a copy of every token, it
     *  compared token bytes and so could never fire on a keyed worker, and the
     *  most a client can do with a duplicate is refuse anyway. This is the
     *  well-behaved side of that contract.</p> */
    public static final class SplitPaginated extends SplitScan {
        @Override public String name() { return "split_paginated"; }
        @Override protected String description() {
            return "Plan enumerated over several disjoint pages";
        }
        @Override protected int perPage() { return 4; }
        @Override protected List<Range> planRanges(long n, long splits) {
            return evenRanges(n, splits);
        }
    }

    /** Pins its plan to a catalog version that has moved on.
     *
     *  <p>The only way a bad split token is reachable through SQL, and
     *  deliberately so: the framework owns the envelope, so a worker cannot mint
     *  a wrong fingerprint or clear a seal even on purpose. What it CAN do is
     *  plan against a snapshot that is no longer current — exactly the situation
     *  SPLIT_SNAPSHOT_EXPIRED names. The refusal must stay distinguishable from
     *  SPLIT_TOKEN_INVALID, because only this one means "re-run the query".</p> */
    public static final class SplitStalePlan extends SplitScan {
        @Override public String name() { return "split_stale_plan"; }
        @Override protected String description() {
            return "Plans against a catalog version that is not the live one";
        }
        // Any value the live catalog will not report. The fixture catalog's
        // version is small, so a large constant is reliably "not current"
        // without depending on what that version happens to be.
        @Override protected Long catalogVersion() { return 987654321L; }
        @Override protected List<Range> planRanges(long n, long splits) {
            return evenRanges(n, splits);
        }
    }

    /** Declares a split-token lifetime shorter than any client's scheduling
     *  horizon.
     *
     *  <p>An expired token is a failed query, not a degradation: nothing
     *  re-plans when one expires, because a distributed engine retries the
     *  serialized task it was handed and has no path back to the planner. So the
     *  only useful moment to notice a too-short lifetime is BEFORE the plan is
     *  issued. One second is unusable everywhere — even DuckDB, whose horizon is
     *  the shortest of any engine because it plans at execution start, can take
     *  longer than that to reach a split.</p> */
    public static final class SplitShortTtl extends SplitScan {
        @Override public String name() { return "split_short_ttl"; }
        @Override protected String description() {
            return "Declares a 1s split-token TTL, below any client horizon";
        }
        @Override protected Long splitTokenTtl() { return 1L; }
        @Override protected List<Range> planRanges(long n, long splits) {
            return evenRanges(n, splits);
        }
    }

    /** Split-capable AND supports_batch_index, which together are a contract.
     *
     *  <p>A batch index must be globally monotonic per reader, and greedy
     *  per-split claiming re-initializes the same connection for each split — so
     *  every split starts a fresh stream, and a worker that restarted its
     *  numbering per split would hand one reader a DECREASING index.</p>
     *
     *  <p>What makes it work is that the client's claim counter hands each reader
     *  strictly ASCENDING split indices, so a worker deriving its index from the
     *  split's position in a globally-ordered space is monotonic by
     *  construction. That is the whole reason claiming is greedy rather than
     *  grouped, and it is NOT something multi-token init provides — a group's
     *  tokens carry no ordering of their own.</p> */
    public static final class SplitBatchIndex extends SplitScan {
        @Override public String name() { return "split_batch_index"; }
        @Override protected String description() {
            return "Split-capable with per-split batch_index space";
        }
        @Override protected long batchStride() { return 1000; }
        @Override protected int maxBatch() { return 64; }
        @Override protected List<Range> planRanges(long n, long splits) {
            return evenRanges(n, splits);
        }
    }

    /** A split scan whose result is cacheable, so never-partial becomes
     *  assertable.
     *
     *  <p>The result cache knows nothing about splits, deliberately: its key
     *  describes the QUERY, while splits are how the rows were produced. What
     *  that makes testable is that a scan abandoned partway — by a LIMIT
     *  satisfied early, or by an error — commits NOTHING: storing what was
     *  captured would put a SUBSET under a key claiming to be the whole answer,
     *  and every later identical query would return missing rows with no error
     *  at all.</p> */
    public static final class SplitCacheable extends SplitScan {
        @Override public String name() { return "split_cacheable"; }
        @Override protected String description() {
            return "Split-capable and cacheable, for the never-partial gate";
        }
        @Override protected Integer cacheTtl() { return 300; }
        @Override protected int maxBatch() { return 16; }
        @Override protected List<Range> planRanges(long n, long splits) {
            return evenRanges(n, splits);
        }
    }

    // =====================================================================
    // Fixtures with shapes of their own
    // =====================================================================

    /**
     * Fails on a chosen split, in either of the two places that matter. They are
     * genuinely different failure paths, not variations:
     *
     * <ul>
     *   <li>{@code fail_in_init} fails while REDEEMING the token, before any row
     *       is produced. The client must not return that connection to the pool
     *       — the init request is on the wire with no answer, so a later
     *       checkout would read this split's init response as its own stream
     *       header: silent cross-query corruption on the {@code pool true}
     *       default.</li>
     *   <li>Otherwise it fails MID-STREAM, after emitting rows, so the capture
     *       is genuinely partial when it dies. A partial result committed as
     *       complete is the failure class the never-partial gate prevents.</li>
     * </ul>
     */
    public static final class SplitFailAt implements TableFunction {

        private static final farm.query.vgi.function.FunctionSpec SPEC =
                farm.query.vgi.function.FunctionSpec.builder("split_fail_at")
                        .metadata(FunctionMetadata.describe(
                                "Fails on a chosen split, at init or mid-stream").withSplits())
                        .named("n", Schemas.INT64, "0")
                        .named("splits", Schemas.INT64, "4")
                        .named("fail_at", Schemas.INT64, "-1")
                        .named("fail_in_init", Schemas.BOOL, "false")
                        .build();

        @Override public farm.query.vgi.function.FunctionSpec spec() { return SPEC; }

        @Override public farm.query.vgi.protocol.BindResponse onBind(TableBindParams params) {
            return farm.query.vgi.protocol.BindResponse.forSchema(
                    farm.query.vgi.internal.SchemaUtil.serializeSchema(SEQ_SCHEMA));
        }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            List<Range> ranges = evenRanges(argLong(params, "n", 0), argLong(params, "splits", 4));
            List<ScanSplit> splits = new ArrayList<>(ranges.size());
            for (int i = 0; i < ranges.size(); i++) {
                Range r = ranges.get(i);
                splits.add(new ScanSplit(encodeOrdinal(i, r), new byte[0], r.hi() - r.lo(), true,
                        null, null, null, null, null, null));
            }
            return PlanResult.of(splits);
        }

        /** Redemption is where the init-time failure lands, so the client's
         *  connection-poisoning path is exercised rather than the mid-stream one. */
        @Override public void onSplit(List<byte[]> payloads, TableBindParams params) {
            // Read through ParameterExtractor rather than the raw named map: the
            // map holds whatever the wire decoder produced, which is not
            // necessarily a java.lang.Boolean, so an identity check against
            // Boolean.TRUE silently answered "false" and the init-failure branch
            // never ran — the fixture failed mid-stream instead, exercising the
            // wrong one of the two paths it exists to distinguish.
            boolean failInInit = farm.query.vgi.function.ParameterExtractor.of(params.arguments())
                    .named("fail_in_init").asBool().orElse(false);
            if (!failInInit) return;
            long failAt = argLong(params, "fail_at", -1);
            for (byte[] p : payloads) {
                long ordinal = decodeOrdinal(p)[0];
                if (ordinal == failAt) {
                    throw new IllegalStateException(
                            "split " + ordinal + " refuses to initialize (fixture)");
                }
            }
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            List<byte[]> payloads = claimed("split_fail_at", params);
            List<Range> ranges = new ArrayList<>(payloads.size());
            List<Long> ordinals = new ArrayList<>(payloads.size());
            for (byte[] p : payloads) {
                long[] o = decodeOrdinal(p);
                ordinals.add(o[0]);
                ranges.add(new Range(o[1], o[2]));
            }
            return new FailState(params, ranges, ordinals, argLong(params, "fail_at", -1));
        }
    }

    /** Walks its claims, failing mid-stream on the chosen split ordinal. */
    public static final class FailState extends TableProducerState {
        /** Ranges this reader claimed. */
        public List<Range> ranges = List.of();
        /** Split ordinals, parallel to {@link #ranges}. */
        public List<Long> ordinals = List.of();
        /** Ordinal to fail on, or -1. */
        public long failAt = -1;
        /** Index of the range being emitted. */
        public int idx;
        /** Next row to emit within the current range. */
        public long cur;

        /** Required no-arg constructor for state deserialization. */
        public FailState() {}

        FailState(TableInitParams params, List<Range> ranges, List<Long> ordinals, long failAt) {
            super(params);
            this.ranges = ranges;
            this.ordinals = ordinals;
            this.failAt = failAt;
            this.cur = ranges.isEmpty() ? 0 : ranges.get(0).lo();
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            while (true) {
                if (idx >= ranges.size()) { out.finish(); return; }
                Range r = ranges.get(idx);
                if (cur >= r.hi()) {
                    idx++;
                    if (idx < ranges.size()) cur = ranges.get(idx).lo();
                    continue;
                }
                // Emit some rows FIRST, so the capture is genuinely partial when
                // it dies rather than empty.
                if (idx < ordinals.size() && ordinals.get(idx) == failAt && cur > r.lo()) {
                    throw new IllegalStateException(
                            "split " + failAt + " failed mid-stream (fixture)");
                }
                int size = (int) Math.min(r.hi() - cur, 8);
                long start = cur;
                cur += size;
                BatchUtil.emit(outputSchema == null ? SEQ_SCHEMA : outputSchema, size, out, null,
                        (root, rows, ignored) -> {
                            BigIntVector v = (BigIntVector) root.getVector("n");
                            v.allocateNew(rows);
                            for (int i = 0; i < rows; i++) v.set(i, start + i);
                        });
                return;
            }
        }
    }

    /**
     * Paginates forever: every plan page returns a cursor and never exhausts it.
     *
     * <p>A worker can hang a client this way by accident as easily as on purpose,
     * and the failure mode is the bad one: a client that stopped early would scan
     * a PARTIAL enumeration and report it as the whole answer. The client must
     * hit its page cap and throw an error naming it — never truncate and
     * proceed.</p>
     */
    public static final class SplitEndlessCursor extends SplitScan {
        @Override public String name() { return "split_endless_cursor"; }
        @Override protected String description() {
            return "Paginates forever: the client must hit its page cap, not truncate";
        }
        @Override protected List<Range> planRanges(long n, long splits) {
            return List.of(new Range(0, 1));
        }
        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            byte[] c = request.cursor();
            int page = c == null ? 0 : c.length;
            byte[] next = new byte[page + 1];
            java.util.Arrays.fill(next, (byte) 'x');
            return PlanResult.of(List.of(new ScanSplit(encode(new Range(0, 1)), new byte[0],
                    null, false, null, null, null, null, null, null))).withNextCursor(next);
        }
    }

    private static final Schema ECHO_SCHEMA = Schemas.of(
            Schemas.nullable("split_ordinal", Schemas.INT64),
            Schemas.nullable("saw_filters", Schemas.BOOL),
            Schemas.nullable("n_projection", Schemas.INT64));

    /**
     * Reports, per split, what pushdown the PLAN call actually received.
     *
     * <p>A row-count assertion cannot catch a pushdown regression — the rows are
     * the same either way — so this makes the pushdown itself the data. What it
     * reports is recorded at PLAN time and baked into each split's payload, which
     * is the claim under test: filters and projection must reach {@code plan()},
     * not merely reach the per-split {@code init()} afterwards.</p>
     */
    public static final class SplitEchoFilters implements TableFunction {

        private static final farm.query.vgi.function.FunctionSpec SPEC =
                farm.query.vgi.function.FunctionSpec.builder("split_echo_filters")
                        .metadata(FunctionMetadata.describe(
                                "Reports, per split, what pushdown the plan call received")
                                // filter_pushdown declares that this worker APPLIES the
                                // filter, so DuckDB stops re-checking it above the scan.
                                // Declaring it while only REPORTING would be the "wrong
                                // answers if declared falsely" hazard in miniature;
                                // autoApply makes the declaration true.
                                .withSplits().withPushdown(false, true, true))
                        .named("splits", Schemas.INT64, "3")
                        .build();

        @Override public farm.query.vgi.function.FunctionSpec spec() { return SPEC; }

        @Override public farm.query.vgi.protocol.BindResponse onBind(TableBindParams params) {
            return farm.query.vgi.protocol.BindResponse.forSchema(
                    farm.query.vgi.internal.SchemaUtil.serializeSchema(ECHO_SCHEMA));
        }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            long sawFilters = request.pushdownFilters() != null
                    && !request.pushdownFilters().filters().isEmpty() ? 1 : 0;
            long nProjection = request.projectionIds() == null ? 0 : request.projectionIds().length;
            long n = argLong(params, "splits", 3);
            List<ScanSplit> splits = new ArrayList<>();
            for (long i = 0; i < n; i++) {
                splits.add(new ScanSplit(
                        encodeOrdinal(i, new Range(sawFilters, nProjection)), new byte[0],
                        null, false, null, null, null, null, null, null));
            }
            return PlanResult.of(splits);
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            List<byte[]> payloads = claimed("split_echo_filters", params);
            List<long[]> rows = new ArrayList<>(payloads.size());
            for (byte[] p : payloads) rows.add(decodeOrdinal(p));
            return new EchoState(params, rows);
        }
    }

    /** One output row per claimed split, carrying what plan() saw. */
    public static final class EchoState extends TableProducerState {
        /** {@code (ordinal, sawFilters, nProjection)} per claimed split. */
        public List<long[]> rows = List.of();
        /** Set once the single batch has been emitted. */
        public boolean done;

        /** Required no-arg constructor for state deserialization. */
        public EchoState() {}

        EchoState(TableInitParams params, List<long[]> rows) {
            super(params);
            this.rows = rows;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            List<long[]> snapshot = rows;
            // Apply the pushdown: this fixture declares autoApplyFilters, which
            // is a PROMISE that the worker filters, and DuckDB stops re-checking
            // above the scan on the strength of it. Emitting unfiltered rows is
            // a wrong-answer bug, not a missed optimization.
            BatchUtil.emitFiltered(outputSchema == null ? ECHO_SCHEMA : outputSchema,
                    snapshot.size(), filters, out, null, (root, n, ignored) -> {
                        BigIntVector ord = (BigIntVector) root.getVector("split_ordinal");
                        org.apache.arrow.vector.BitVector saw =
                                (org.apache.arrow.vector.BitVector) root.getVector("saw_filters");
                        BigIntVector proj = (BigIntVector) root.getVector("n_projection");
                        if (ord != null) ord.allocateNew(n);
                        if (saw != null) saw.allocateNew(n);
                        if (proj != null) proj.allocateNew(n);
                        for (int i = 0; i < n; i++) {
                            long[] r = snapshot.get(i);
                            if (ord != null) ord.set(i, r[0]);
                            if (saw != null) saw.set(i, r[1] != 0 ? 1 : 0);
                            if (proj != null) proj.set(i, r[2]);
                        }
                    });
        }
    }

    static final String[] COUNTRIES = {"US", "DE", "JP", "BR"};

    private static final Schema PART_SCHEMA = Schemas.of(
            EmitMetadata.partitionField("country", Schemas.UTF8),
            Schemas.nullable("sales", Schemas.INT64));

    /**
     * One split per partition — the shape a partitioned table naturally takes.
     *
     * <p>A partition and a split are different things that usually coincide: a
     * partition is a property of the DATA (every row here shares a value), a
     * split is a unit of WORK. A worker that already stores data per partition
     * has its split boundaries handed to it, so this is the common case rather
     * than a contrived one.</p>
     *
     * <p>What needs asserting is that the two survive each other. Splits are
     * claimed greedily, in an order nobody chose, by readers that each end up
     * holding several — so the association between a batch and the partition
     * value it carries has to hold through re-init on a reused connection and
     * across the boundary where one reader moves from one partition to the next.
     * Losing it does not raise: it produces a GROUP BY that silently mixes
     * partitions.</p>
     */
    public static final class SplitPartitioned implements TableFunction {

        private static final farm.query.vgi.function.FunctionSpec SPEC =
                farm.query.vgi.function.FunctionSpec.builder("split_partitioned")
                        .metadata(FunctionMetadata.describe(
                                "One split per partition, with partition values on each batch")
                                .withSplits()
                                .withPartitionKind(FunctionMetadata.PartitionKind.SINGLE_VALUE_PARTITIONS))
                        .named("rows_per_country", Schemas.INT64, "5")
                        .build();

        @Override public farm.query.vgi.function.FunctionSpec spec() { return SPEC; }

        @Override public farm.query.vgi.protocol.BindResponse onBind(TableBindParams params) {
            return farm.query.vgi.protocol.BindResponse.forSchema(
                    farm.query.vgi.internal.SchemaUtil.serializeSchema(PART_SCHEMA));
        }

        /** Names each partition by INDEX, so a redemption reads the same
         *  partition however many times it runs and in whichever process. */
        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            List<ScanSplit> splits = new ArrayList<>(COUNTRIES.length);
            for (int i = 0; i < COUNTRIES.length; i++) {
                splits.add(new ScanSplit(encode(new Range(i, i)), new byte[0],
                        null, false, null, null, null, null, null, null));
            }
            return PlanResult.of(splits);
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            List<byte[]> payloads = claimed("split_partitioned", params);
            List<Long> idxs = new ArrayList<>(payloads.size());
            for (byte[] p : payloads) idxs.add(decode(p).lo());
            return new PartState(params, idxs, argLong(params, "rows_per_country", 5));
        }
    }

    /** Emits one partition per tick, tagged with its partition values. */
    public static final class PartState extends TableProducerState {
        /** Partition indices this reader claimed. */
        public List<Long> indices = List.of();
        /** How far through {@link #indices} this reader is. */
        public int at;
        /** Rows to emit per partition. */
        public long rows;

        /** Required no-arg constructor for state deserialization. */
        public PartState() {}

        PartState(TableInitParams params, List<Long> indices, long rows) {
            super(params);
            this.indices = indices;
            this.rows = rows;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            // A partition with zero rows is STEPPED OVER, never reported as
            // end-of-stream — the same rule every split fixture follows, and here
            // it is reachable through `rows_per_country := 0`.
            while (true) {
                if (at >= indices.size()) { out.finish(); return; }
                int ci = (int) (long) indices.get(at);
                at++;
                if (rows <= 0 || ci < 0 || ci >= COUNTRIES.length) continue;
                // Each partition's values are offset by its own index, so
                // swapping two splits' labels MOVES the per-partition sums. With
                // identical values everywhere a mislabelled partition would be
                // invisible in the totals.
                long base = ci * 100L;
                String country = COUNTRIES[ci];
                int n = (int) rows;
                // The partition metadata is derived from the BUILT batch (min/max
                // per annotated column), so the root has to exist before it can
                // be computed — which is why this builds the root by hand rather
                // than going through BatchUtil.emit's metadata-up-front form.
                org.apache.arrow.vector.VectorSchemaRoot root =
                        org.apache.arrow.vector.VectorSchemaRoot.create(
                                PART_SCHEMA, farm.query.vgirpc.wire.Allocators.root());
                boolean emitted = false;
                try {
                    root.allocateNew();
                    VarCharVector c = (VarCharVector) root.getVector("country");
                    BigIntVector s = (BigIntVector) root.getVector("sales");
                    org.apache.arrow.vector.util.Text text =
                            new org.apache.arrow.vector.util.Text(country);
                    for (int i = 0; i < n; i++) {
                        c.setSafe(i, text);
                        s.setSafe(i, base + i + 1);
                    }
                    root.setRowCount(n);
                    // SINGLE_VALUE: min == max within the batch, which is what
                    // lets the client read row 0 as the exact partition key.
                    out.emit(root, EmitMetadata.partitionValues(PART_SCHEMA, root, null));
                    emitted = true;
                } finally {
                    if (!emitted) root.close();
                }
                return;
            }
        }
    }

    private static final Schema DYN_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("pushed_filters", Schemas.UTF8));

    /**
     * Echoes the DYNAMIC filter each tick carried, per split.
     *
     * <p>A plan is built from STATIC filters only — join-key values are not known
     * when the plan RPC fires, so they cannot prune the split SET. They arrive
     * later, per tick, and prune WITHIN each split. Both halves have to keep
     * working once a reader re-initializes the same connection per split: the
     * tick filter state is a property of the connection, and a split that lost it
     * would silently stop pruning.</p>
     *
     * <p>"Silently" is the operative word, and it is why this reports the filter
     * as DATA rather than leaving the test to infer it from row counts. A scan
     * that stopped receiving dynamic filters returns exactly the same rows —
     * DuckDB re-checks the predicate above the scan — just after shipping more of
     * them.</p>
     */
    public static final class SplitDynamicFilter implements TableFunction {

        private static final farm.query.vgi.function.FunctionSpec SPEC =
                farm.query.vgi.function.FunctionSpec.builder("split_dynamic_filter")
                        .metadata(FunctionMetadata.describe(
                                "Echoes the dynamic filter each tick carried, per split")
                                .withSplits().withPushdown(true, true, true))
                        .named("n", Schemas.INT64, "0")
                        .named("splits", Schemas.INT64, "4")
                        .build();

        @Override public farm.query.vgi.function.FunctionSpec spec() { return SPEC; }

        @Override public farm.query.vgi.protocol.BindResponse onBind(TableBindParams params) {
            return farm.query.vgi.protocol.BindResponse.forSchema(
                    farm.query.vgi.internal.SchemaUtil.serializeSchema(DYN_SCHEMA));
        }

        /**
         * Report the row count, which decides which side of a join this lands on.
         *
         * <p>Without it DuckDB assumes a default (large) cardinality and puts the
         * scan on the BUILD side of a hash join — where no join-key IN filter is
         * pushed into it, because the filter goes to the probe side. The scan
         * then reads everything and DuckDB filters above it: right answers, no
         * pushdown, and nothing in the result to say so. Nothing about splits
         * causes that; it is the ordinary consequence of a table function
         * declining to estimate itself.</p>
         */
        @Override public long cardinality(TableBindParams params) {
            return argLong(params, "n", 0);
        }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            List<Range> ranges = evenRanges(argLong(params, "n", 0), argLong(params, "splits", 4));
            List<ScanSplit> splits = new ArrayList<>(ranges.size());
            for (Range r : ranges) {
                splits.add(new ScanSplit(encode(r), new byte[0],
                        null, false, null, null, null, null, null, null));
            }
            return PlanResult.of(splits);
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            List<byte[]> payloads = claimed("split_dynamic_filter", params);
            List<Range> ranges = new ArrayList<>(payloads.size());
            for (byte[] p : payloads) ranges.add(decode(p));
            // Decode with the JOIN KEYS: merging them is what produces the IN
            // filter a join pushes down. Without them a join renders as
            // "(none)" — the pushdown arrived and the fixture could not see it.
            PushdownFilters pf = farm.query.vgi.pushdown.PushdownFiltersDecoder.decode(
                    params.pushdownFilters(), params.joinKeys());
            return new DynState(params, ranges, renderFiltersCanonical(pf));
        }
    }

    /** Walks its claims, stamping every row with the filter this init carried. */
    public static final class DynState extends TableProducerState {
        /** Ranges this reader claimed. */
        public List<Range> ranges = List.of();
        /** Index of the range being emitted. */
        public int idx;
        /** Next row to emit within the current range. */
        public long cur;
        /** The canonical rendering of the pushdown this init received. */
        public String rendered = "(none)";

        /** Required no-arg constructor for state deserialization. */
        public DynState() {}

        DynState(TableInitParams params, List<Range> ranges, String rendered) {
            super(params);
            this.ranges = ranges;
            this.rendered = rendered;
            this.cur = ranges.isEmpty() ? 0 : ranges.get(0).lo();
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            while (true) {
                if (idx >= ranges.size()) { out.finish(); return; }
                Range r = ranges.get(idx);
                if (cur >= r.hi()) {
                    idx++;
                    if (idx < ranges.size()) cur = ranges.get(idx).lo();
                    continue;
                }
                int size = (int) Math.min(r.hi() - cur, 4);
                long start = cur;
                cur += size;
                String label = rendered;
                // Apply the pushdown to the emitted batch. Declaring
                // autoApplyFilters is a PROMISE that the worker applies it —
                // DuckDB stops re-checking above the scan on the strength of it —
                // so emitting unfiltered rows here is a wrong-answer bug, not a
                // missed optimization. BatchUtil.emit does not do it for us.
                BatchUtil.emitFiltered(outputSchema == null ? DYN_SCHEMA : outputSchema, size,
                        filters, out, null,
                        (root, rows, ignored) -> {
                            BigIntVector v = (BigIntVector) root.getVector("n");
                            VarCharVector f = (VarCharVector) root.getVector("pushed_filters");
                            if (v != null) {
                                v.allocateNew(rows);
                                for (int i = 0; i < rows; i++) v.set(i, start + i);
                            }
                            if (f != null) {
                                f.allocateNew(rows);
                                byte[] bytes = label.getBytes(StandardCharsets.UTF_8);
                                for (int i = 0; i < rows; i++) f.setSafe(i, bytes);
                            }
                        });
                return;
            }
        }
    }
}
