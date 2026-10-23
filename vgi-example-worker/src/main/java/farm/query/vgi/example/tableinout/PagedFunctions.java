// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.tableinout.RowTransformFunction;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Continued-output fixtures: one input batch answered across several replies
 * ({@code table_in_out/lateral_continuation.test}, {@code input_rows_per_call.test},
 * {@code continuation_classic.test}, {@code continuation_resume_error_revalidate.test},
 * {@code continuation_cache.test}; contracts in those files' headers).
 *
 * <p>Each emits {@code blended_explode}'s output (rows {@code 0..n-1} per input
 * row) {@code page_size} rows per reply: {@code onInputBatch} emits the first
 * with more output and {@code resume} the rest, the way a function backed by a
 * paged API would. {@link PagedState} holds only the counts and a position,
 * never output rows, so it stays small in an HTTP state token. Mirrors
 * vgi-python's {@code BlendedPagedFunction}, {@code BlendedSlicedFunction} and
 * {@code PagedExplodeFunction}.
 */
public final class PagedFunctions {

    private PagedFunctions() {}

    private static final Schema I_SCHEMA = new Schema(List.of(
            new Field("i", new FieldType(true, Schemas.INT64, null), null)));

    private static final Schema SLICED_SCHEMA = new Schema(List.of(
            new Field("i", new FieldType(true, Schemas.INT64, null), null),
            new Field("call_rows", new FieldType(true, Schemas.INT64, null), null)));

    private static FunctionSpec.Builder pagedArgs(FunctionSpec.Builder b) {
        return b.named("page_size", Schemas.INT64, "2")
                .named("cache_ttl", Schemas.INT64, "0")
                .named("fail_on_page", Schemas.INT64, "0")
                .named("revalidate", Schemas.BOOL, "false")
                .named("not_modified_more", Schemas.BOOL, "false");
    }

    private static long longArg(Arguments args, String name, long defaultValue) {
        return ParameterExtractor.of(args).named(name).asLong().orElse(defaultValue);
    }

    private static boolean boolArg(Arguments args, String name) {
        return ParameterExtractor.of(args).named(name).asBool().orElse(false);
    }

    private static void checkBind(Arguments args, long defaultPageSize) {
        long pageSize = longArg(args, "page_size", defaultPageSize);
        if (pageSize < 1) throw new IllegalArgumentException("page_size must be at least 1, got " + pageSize);
        long ttl = longArg(args, "cache_ttl", 0);
        if (ttl < 0) throw new IllegalArgumentException("cache_ttl must not be negative, got " + ttl);
        // vgi.cache.ttl is an int32 count of seconds.
        if (ttl > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("cache_ttl must be at most " + Integer.MAX_VALUE + ", got " + ttl);
        }
    }

    /**
     * {@code blended_paged(n BIGINT, page_size := 2, cache_ttl := 0, fail_on_page := 0,
     * revalidate := false, not_modified_more := false) -> i} — blended 1-&gt;N fan-out
     * with {@code parent_rows} provenance, {@code page_size} rows per reply.
     */
    public static final class BlendedPagedFunction implements RowTransformFunction {

        private static final FunctionSpec SPEC = pagedArgs(FunctionSpec.builder("blended_paged")
                .metadata(FunctionMetadata.describe(
                                "Blended 1->N fan-out (emit 0..n-1 per input row), page_size rows per reply")
                        .withCategories("blended", "test"))
                .arg("n", Schemas.INT64))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public Class<PagedState> continuationState() { return PagedState.class; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            checkBind(params.arguments(), 2);
            return BindResponse.forSchema(SchemaUtil.serializeSchema(I_SCHEMA));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new PagedState(PagedState.BLENDED, params.arguments(), 2);
        }
    }

    /**
     * {@code blended_sliced(n BIGINT, page_size := 1000000, cache_ttl := 0) -> (i, call_rows)}
     * — {@code blended_paged} declaring {@code input_rows_per_call = 3}; fails a call
     * of more than 3 rows and reports each call's row count in {@code call_rows}.
     * With {@code cache_ttl > 0} each call's first reply is per-value cacheable.
     */
    public static final class BlendedSlicedFunction implements RowTransformFunction {

        private static final FunctionSpec SPEC = FunctionSpec.builder("blended_sliced")
                .metadata(FunctionMetadata.describe(
                                "Blended 1->N fan-out taking at most 3 input rows per call, reporting each call's size")
                        .withCategories("blended", "test"))
                .arg("n", Schemas.INT64)
                .named("page_size", Schemas.INT64, "1000000")
                .named("cache_ttl", Schemas.INT64, "0")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public Long inputRowsPerCall() { return 3L; }

        @Override public Class<PagedState> continuationState() { return PagedState.class; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            checkBind(params.arguments(), 1_000_000);
            return BindResponse.forSchema(SchemaUtil.serializeSchema(SLICED_SCHEMA));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new PagedState(PagedState.SLICED, params.arguments(), 1_000_000);
        }
    }

    /**
     * {@code paged_explode(data TABLE, page_size := 2, cache_ttl := 0, fail_on_page := 0,
     * revalidate := false, not_modified_more := false) -> i} — the classic
     * (TABLE-input) counterpart of {@code blended_paged}: the input's first column
     * is the count, and it runs through the stock in-out path.
     */
    public static final class PagedExplodeFunction implements TableInOutFunction {

        private static final FunctionSpec SPEC = pagedArgs(FunctionSpec.builder("paged_explode")
                .metadata(FunctionMetadata.describe("Classic table-in-out 1->N fan-out, page_size rows per reply")
                        .withCategories("table_in_out", "test"))
                .table("data"))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public Class<PagedState> continuationState() { return PagedState.class; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            checkBind(params.arguments(), 2);
            return BindResponse.forSchema(SchemaUtil.serializeSchema(I_SCHEMA));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new PagedState(PagedState.TABLE, params.arguments(), 2);
        }
    }

    /**
     * Where the next reply resumes: the input's fan-out counts and a position in
     * them, plus the bind-time arguments. Public fields + no-arg constructor for
     * the HTTP state-token round trip.
     */
    public static final class PagedState extends TableInOutExchangeState {
        static final int BLENDED = 0;
        static final int SLICED = 1;
        static final int TABLE = 2;

        /** Which fixture this is: {@link #BLENDED}, {@link #SLICED} or {@link #TABLE}. */
        public int kind;
        /** Output rows per reply. */
        public long pageSize;
        /** When &gt; 0, the first reply's {@code vgi.cache.ttl}. */
        public long cacheTtl;
        /** When &gt; 0, the (1-based) reply to fail instead of sending. */
        public long failOnPage;
        /** Answer conditional requests with changed, paged output. */
        public boolean revalidate;
        /** With {@link #revalidate}, answer a conditional request with an empty not_modified reply that says more. */
        public boolean notModifiedMore;
        /** The current input's fan-out counts (NULL / negative as 0). */
        public long[] counts = new long[0];
        /** The input row the next reply starts in. */
        public int row;
        /** The next value within {@link #row}. */
        public long offset;
        /** blended_sliced: the current call's input row count. */
        public long callRows;
        /** Replies sent for the current input. */
        public long page;
        /** not_modified_more: the first reply is empty and says more. */
        public boolean emptyFirst;
        /** Added to every output value (revalidate: the data changed); -1 = one reply, unchanged. */
        public long bump;

        /** No-arg constructor for HTTP state-token deserialization. */
        public PagedState() {}

        PagedState(int kind, Arguments args, long defaultPageSize) {
            this.kind = kind;
            this.pageSize = longArg(args, "page_size", defaultPageSize);
            this.cacheTtl = longArg(args, "cache_ttl", 0);
            this.failOnPage = longArg(args, "fail_on_page", 0);
            this.revalidate = boolArg(args, "revalidate");
            this.notModifiedMore = boolArg(args, "not_modified_more");
        }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot in = input.root();
            int rows = in.getRowCount();
            FieldVector col = kind == TABLE
                    ? (in.getFieldVectors().isEmpty() ? null : in.getVector(0))
                    : in.getVector("n");
            counts = new long[col == null ? 0 : rows];
            for (int r = 0; r < counts.length; r++) {
                counts[r] = col.isNull(r) ? 0 : Math.max(0, ((BaseIntVector) col).getValueAsLong(r));
            }
            row = 0;
            offset = 0;
            page = 0;
            bump = 0;
            emptyFirst = false;
            CacheControl cc;
            if (kind == SLICED) {
                if (rows > 3) {
                    throw new IllegalStateException(
                            "blended_sliced declares input_rows_per_call = 3 but got " + rows + " rows");
                }
                callRows = rows;
                cc = cacheTtl > 0
                        ? CacheControl.builder().ttl(Math.toIntExact(cacheTtl)).perValue(true).build()
                        : null;
            } else {
                cc = begin(input.customMetadata().get(CacheControl.IF_NONE_MATCH_KEY));
            }
            emitPage(out, cc);
        }

        @Override public void resume(OutputCollector out, CallContext ctx) {
            emitPage(out, null);
        }

        /**
         * Start a new input; return its first reply's cache control. Without
         * {@code revalidate} that is {@code cache_ttl}'s, if any. With it the
         * output depends on the request alone: a plain request gets the whole
         * output in one reply (ttl 0, etag v1, revalidatable); a conditional one
         * is told the data changed (every value 1000 larger, paged, etag v2), or,
         * with {@code not_modified_more}, first gets an empty not_modified reply
         * that says more — a protocol violation the client must refuse.
         */
        private CacheControl begin(String ifNoneMatch) {
            if (!revalidate) return cacheTtl > 0 ? CacheControl.ttl(Math.toIntExact(cacheTtl)) : null;
            if (ifNoneMatch == null) {
                bump = -1;
                return CacheControl.builder().ttl(0).etag("v1").revalidatable(true).build();
            }
            bump = 1000;
            if (notModifiedMore) {
                emptyFirst = true;
                return CacheControl.builder().notModified(true).ttl(0).etag("v1").revalidatable(true).build();
            }
            return CacheControl.builder().ttl(0).etag("v2").revalidatable(true).build();
        }

        private void emitPage(OutputCollector out, CacheControl cc) {
            List<Long> values = new ArrayList<>();
            List<Integer> parents = new ArrayList<>();
            boolean more;
            if (kind == SLICED) {
                more = nextPage(pageSize, values, parents);
            } else {
                page++;
                if (page == failOnPage) throw new IllegalStateException("failing on page " + page + " as asked");
                if (page == 1 && emptyFirst) {
                    more = true;
                } else if (bump < 0) {
                    more = nextPage(Long.MAX_VALUE, values, parents);
                } else {
                    more = nextPage(pageSize, values, parents);
                    values.replaceAll(v -> v + bump);
                }
            }
            int n = values.size();
            VectorSchemaRoot outRoot = VectorSchemaRoot.create(kind == SLICED ? SLICED_SCHEMA : I_SCHEMA,
                    Allocators.root());
            outRoot.allocateNew();
            BigIntVector is = (BigIntVector) outRoot.getVector("i");
            for (int k = 0; k < n; k++) is.setSafe(k, values.get(k));
            if (kind == SLICED) {
                BigIntVector callRowsVec = (BigIntVector) outRoot.getVector("call_rows");
                for (int k = 0; k < n; k++) callRowsVec.setSafe(k, callRows);
            }
            outRoot.setRowCount(n);
            Map<String, String> md = cc == null ? null : cc.toMetadata();
            if (kind != TABLE) {
                md = RowTransformFunction.parentRows(parents.stream().mapToInt(Integer::intValue).toArray(), n, md);
            }
            out.emit(outRoot, withMoreOutput(more, md));
        }

        /**
         * Append the next {@code size} output rows (0..n-1 for each input row in
         * order, so a page may span rows) and return whether more follow. Rows
         * with no output after the last non-empty page are skipped, so the last
         * page is never followed by an empty one.
         */
        private boolean nextPage(long size, List<Long> values, List<Integer> parents) {
            while (row < counts.length && values.size() < size) {
                long take = Math.min(counts[row] - offset, size - values.size());
                for (long v = offset; v < offset + take; v++) {
                    values.add(v);
                    parents.add(row);
                }
                offset += take;
                if (offset == counts[row]) {
                    row++;
                    offset = 0;
                }
            }
            while (row < counts.length && counts[row] == 0) row++;
            return row < counts.length;
        }
    }
}
