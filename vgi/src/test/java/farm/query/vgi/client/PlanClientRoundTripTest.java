// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.protocol.TableFunctionPlanRequest;
import farm.query.vgi.table.PlanRequest;
import farm.query.vgi.table.PlanResult;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end exercise of the {@code table_function_plan} RPC from the
 * <em>client</em> side: plan a scan across several paginated calls, redeem
 * every returned split, and drain each one.
 *
 * <p>Before this test nothing on the client side built a
 * {@code table_function_plan} request or read the resulting splits — the
 * wire schema existed only inside the worker's own dispatch
 * ({@code VgiServiceImpl.table_function_plan}), unreachable from outside the
 * process. {@link TableFunctionPlanRequest} closes that gap the same way
 * {@link VgiClientRoundTripTest} closed it for {@code bind}/{@code init}, and
 * this test is the reference for a JVM consumer (Trino, Spark, Flink) driving
 * split-based parallelism: call {@code table_function_plan} with a pagination
 * cap, follow {@code next_cursors} until exhausted, then redeem each
 * {@link ScanSplit} by its {@code token} via {@code init()}.</p>
 *
 * <p>It also proves the server-side fix alongside it: {@code
 * max_splits_per_response}/{@code min_splits}/{@code target_split_bytes}/
 * {@code projection_ids} used to be silently dropped by {@code
 * VgiServiceImpl#planRequestOf} even though {@link PlanRequest} already had
 * slots for three of them — a client-supplied pagination cap had no effect
 * on the wire. This test fails if that regresses.</p>
 */
final class PlanClientRoundTripTest {

    // ------------------------------------------------------------------
    // Fixture: a split-capable table function emitting 0..n-1, divided into
    // `splits` even ranges. Self-contained (rather than depending on
    // vgi-example-worker's SplitFunctions, a different Gradle module) so this
    // test exercises only what the `vgi` module itself ships.
    // ------------------------------------------------------------------

    /** The half-open range {@code [lo, hi)} one split owns. */
    private record Range(long lo, long hi) {}

    private static byte[] encode(Range r) {
        return ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(r.lo()).putLong(r.hi()).array();
    }

    private static Range decode(byte[] payload) {
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        return new Range(b.getLong(), b.getLong());
    }

    /** Divide {@code [0, n)} into {@code k} contiguous ranges, remainder first. */
    private static List<Range> evenRanges(long n, long k) {
        List<Range> out = new ArrayList<>();
        if (k <= 0) return out;
        long base = n / k;
        long extra = n % k;
        long lo = 0;
        for (long i = 0; i < k; i++) {
            long hi = lo + base + (i < extra ? 1 : 0);
            out.add(new Range(lo, hi));
            lo = hi;
        }
        return out;
    }

    private static long namedLong(TableBindParams params, String name) {
        Object v = params.arguments().named().get(name);
        return v instanceof Number num ? num.longValue() : 0L;
    }

    /** Emits {@code 0..n-1}, split into {@code splits} ranges, paginated via {@code plan()}. */
    static final class PlanSeqFunction implements TableFunction {

        static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

        @Override public String name() { return "plan_seq"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(
                    "Split-capable 0..n-1, for exercising table_function_plan pagination").withSplits();
        }

        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    ArgSpec.named("n", Schemas.INT64, "0"),
                    ArgSpec.named("splits", Schemas.INT64, "1"));
        }

        @Override public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT_SCHEMA));
        }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            long n = namedLong(params, "n");
            long splits = namedLong(params, "splits");
            List<Range> all = evenRanges(n, splits);

            long cap = request.maxSplitsPerResponse() == null
                    ? all.size() : request.maxSplitsPerResponse();
            long page = request.cursor() == null || request.cursor().length == 0
                    ? 0 : ByteBuffer.wrap(request.cursor()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            int lo = (int) Math.min(page * cap, all.size());
            int hi = (int) Math.min(lo + cap, all.size());

            List<ScanSplit> splitList = new ArrayList<>();
            for (Range r : all.subList(lo, hi)) {
                long rows = r.hi() - r.lo();
                splitList.add(new ScanSplit(encode(r), new byte[0], rows, true, rows * 8,
                        null, null, null, null, null));
            }
            PlanResult result = PlanResult.of(splitList);
            if (hi < all.size()) {
                result = result.withNextCursor(ByteBuffer.allocate(8)
                        .order(ByteOrder.LITTLE_ENDIAN).putLong(page + 1).array());
            }
            return result;
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            List<byte[]> payloads = params.splitPayloads();
            List<Range> ranges = new ArrayList<>();
            if (payloads != null) {
                for (byte[] p : payloads) ranges.add(decode(p));
            }
            return new SeqState(ranges);
        }
    }

    /** Emits every row of its claimed ranges in a single batch, then finishes. */
    public static final class SeqState extends TableProducerState {
        /** The ranges this reader claimed. */
        public List<Range> ranges = List.of();
        /** Whether this reader has already produced its one batch. */
        public boolean emitted;

        /** Required no-arg constructor for state deserialization. */
        public SeqState() {}

        SeqState(List<Range> ranges) { this.ranges = ranges; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            emitted = true;
            int total = 0;
            for (Range r : ranges) total += (int) (r.hi() - r.lo());
            if (total == 0) { out.finish(); return; }
            BatchUtil.emit(PlanSeqFunction.OUTPUT_SCHEMA, total, out, (root, rows, ignored) -> {
                BigIntVector v = (BigIntVector) root.getVector("n");
                v.allocateNew(rows);
                int idx = 0;
                for (Range r : ranges) {
                    for (long x = r.lo(); x < r.hi(); x++) v.set(idx++, x);
                }
            });
        }
    }

    // ------------------------------------------------------------------
    // The round trip: plan across several paginated calls, redeem every split.
    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void plansAndRedeemsEverySplitAcrossPagination() throws Exception {
        Worker worker = Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerTable(new PlanSeqFunction());

        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker)) {
            VgiService vgi = h.client();

            byte[] handle = vgi.catalog_attach(
                    CatalogAttachRequest.of("testcat", null, null, null), null)
                    .attach_opaque_data();

            BindRequest bindRequest = new BindRequest(
                    "plan_seq",
                    ArgumentsEncoder.builder().named("n", 20L).named("splits", 6L).encode(),
                    "TABLE", null, null, null, handle, null, false,
                    null, null, null, null, "main");
            BindResponse bound = vgi.bind(bindRequest, null);
            byte[] serializedBindCall = RecordCodec.serializeToBytes(bindRequest);

            // PAGINATE — cap 2 splits per response over 6 total, so this must
            // take three round trips and follow next_cursors between them.
            List<byte[]> tokens = new ArrayList<>();
            TableFunctionPlanRequest planReq = TableFunctionPlanRequest
                    .of(serializedBindCall, bound.opaque_data())
                    .withMaxSplitsPerResponse(2);
            int pages = 0;
            while (true) {
                pages++;
                PlanResponse resp = vgi.table_function_plan(
                        RecordCodec.serializeToBytes(planReq), null);
                assertTrue(resp.splits().size() <= 2, "must honour the pagination cap");
                for (byte[] blob : resp.splits()) {
                    ScanSplit split = RecordCodec.deserializeFromBytes(blob, ScanSplit.class);
                    assertTrue(split.token().length > 0, "framework must stamp a token");
                    tokens.add(split.token());
                }
                if (resp.next_cursors() == null || resp.next_cursors().isEmpty()) break;
                assertTrue(pages < 10, "runaway pagination — next_cursors never emptied");
                planReq = planReq.withCursor(resp.next_cursors().get(0), 2);
            }
            assertEquals(3, pages, "6 splits at 2 per page must take exactly 3 calls");
            assertEquals(6, tokens.size(), "all 6 splits across every page");

            // REDEEM every split and drain it, collecting the union of rows.
            TreeSet<Long> rows = new TreeSet<>();
            for (byte[] token : tokens) {
                InitRequest initRequest = new InitRequest(
                        serializedBindCall, bound.output_schema(), bound.opaque_data(),
                        null, null, null, null, null, null,
                        null, null, null, null, null, null,
                        null, null, List.of(token), null);
                RpcStream<? extends StreamState> stream = vgi.init(initRequest, null);
                GlobalInitResponse header = (GlobalInitResponse) stream.header();
                assertNotNull(header, "init must return a GlobalInitResponse header");
                rows.addAll(drainInt64Column(stream, "n"));
            }
            assertEquals(20, rows.size(), "every row of 0..19 exactly once across all splits");
            assertEquals(0L, rows.first());
            assertEquals(19L, rows.last());
        }
    }

    private static List<Long> drainInt64Column(RpcStream<? extends StreamState> stream, String column) {
        List<Long> out = new ArrayList<>();
        ClientStreamSession<?> session = (ClientStreamSession<?>) stream;
        try {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = session.tick();
                } catch (NoSuchElementException endOfStream) {
                    break;
                }
                VectorSchemaRoot root = batch.root();
                BigIntVector v = (BigIntVector) root.getVector(column);
                for (int i = 0; i < root.getRowCount(); i++) out.add(v.get(i));
            }
        } finally {
            session.close();
        }
        return out;
    }
}
