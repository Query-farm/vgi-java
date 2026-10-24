// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.tableinout.PassthroughTIOFunction;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Continued table-in-out output through real transports: the pipe transport
 * (vgi-rpc's {@code RpcServer} tick loop, which writes whatever a turn emitted)
 * and HTTP ({@code HttpStreamHandler}, where every turn is its own request, the
 * exchange's pending flag rides the state token, and the input's transport
 * metadata is stripped before the exchange sees it).
 *
 * <p>Over HTTP the tick's {@code vgi.status: CONTINUE} must survive that
 * stripping: were it removed, the tick would reach the exchange as a real
 * input while output is pending and be refused. The zero-emit cases would hang
 * the pipe transport (the client waits for a reply that is never written)
 * without the exchange's own refusal, so every test here runs under a timeout.
 */
@Timeout(60)
final class ContinuedOutputTransportTest {

    private static final Schema SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

    enum Transport { PIPE, HTTP }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    /** Echoes each input row's value, two rows per reply. */
    public static final class Pages extends TableInOutExchangeState {
        public long[] values = new long[0];
        public int next;

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            BigIntVector n = (BigIntVector) input.root().getVector("n");
            values = new long[input.root().getRowCount()];
            for (int i = 0; i < values.length; i++) values[i] = n.get(i);
            next = 0;
            emitPage(out);
        }

        @Override public void resume(OutputCollector out, CallContext ctx) {
            emitPage(out);
        }

        void emitPage(OutputCollector out) {
            int end = Math.min(values.length, next + 2);
            out.emit(rows(java.util.Arrays.copyOfRange(values, next, end)), withMoreOutput(end < values.length, null));
            next = end;
        }
    }

    /** Says more, then answers the tick with nothing, like a resume() returning early at end of data. */
    public static final class SilentResume extends TableInOutExchangeState {
        public boolean started;

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            started = true;
            out.emit(rows(1), withMoreOutput(true, null));
        }

        @Override public void resume(OutputCollector out, CallContext ctx) {
            // End of data: returns without emitting the last (empty) reply.
        }
    }

    /** A plain exchange answering its input with nothing. */
    public static final class SilentInput extends TableInOutExchangeState {
        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {}
    }

    /** Says more, then fails while that output is pending. */
    public static final class FailingResume extends TableInOutExchangeState {
        public int page;

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            page = 1;
            out.emit(rows(1), withMoreOutput(true, null));
        }

        @Override public void resume(OutputCollector out, CallContext ctx) {
            throw new IllegalStateException("failing on page " + (++page) + " as asked");
        }
    }

    /** A passthrough (n BIGINT -&gt; n BIGINT) function over one exchange class. */
    private static final class Fn extends PassthroughTIOFunction {
        private final FunctionSpec spec;
        private final Class<? extends TableInOutExchangeState> continuation;
        private final Supplier<TableInOutExchangeState> exchange;

        Fn(String name, Class<? extends TableInOutExchangeState> continuation,
           Supplier<TableInOutExchangeState> exchange) {
            this.spec = FunctionSpec.builder(name).description("continued-output transport probe").build();
            this.continuation = continuation;
            this.exchange = exchange;
        }

        @Override public FunctionSpec spec() { return spec; }
        @Override public Class<? extends TableInOutExchangeState> continuationState() { return continuation; }
        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) { return exchange.get(); }
    }

    private static Worker worker() {
        return Worker.builder().catalogName("cont").defaultSchema("main")
                .registerTableInOut("main", new Fn("pages", Pages.class, Pages::new))
                .registerTableInOut("main", new Fn("silent_resume", SilentResume.class, SilentResume::new))
                .registerTableInOut("main", new Fn("silent_input", null, SilentInput::new))
                .registerTableInOut("main", new Fn("failing_resume", FailingResume.class, FailingResume::new));
    }

    // ------------------------------------------------------------------
    // Connections
    // ------------------------------------------------------------------

    private final List<AutoCloseable> open = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = open.size() - 1; i >= 0; i--) open.get(i).close();
    }

    private VgiService connect(Transport t) throws Exception {
        if (t == Transport.PIPE) {
            PipeWorkerHarness h = PipeWorkerHarness.start(worker());
            open.add(h);
            return h.client();
        }
        HttpServer http = new HttpServer(worker().rpcServer(),
                HttpServer.Config.builder().host("127.0.0.1").port(0).build());
        http.start();
        open.add(http::stop);
        HttpRpcConnection c = HttpRpcConnection.builder("http://127.0.0.1:" + http.port()).build();
        open.add(c::close);
        return c.proxy(VgiService.class);
    }

    /** Bind and open the exchange stream of {@code function}. */
    private static RpcStream<?> open(VgiService vgi, String function) {
        byte[] handle = vgi.catalog_attach(CatalogAttachRequest.of("cont", null, null, null), null)
                .attach_opaque_data();
        BindRequest bind = new BindRequest(function, null, "TABLE", SchemaUtil.serializeSchema(SCHEMA),
                null, null, handle, null, false, null, null, null, null, "main");
        BindResponse bound = vgi.bind(bind, null);
        return vgi.init(new InitRequest(RecordCodec.serializeToBytes(bind), bound.output_schema(),
                bound.opaque_data(), null, null, null, "INPUT", null, null, null, null, null, null, null, null,
                null, null, null, null), null);
    }

    private record Reply(List<Long> rows, boolean more) {}

    private static Reply send(RpcStream<?> stream, long... values) {
        return send(stream, Map.of(), values);
    }

    private static Reply tick(RpcStream<?> stream) {
        return send(stream, Map.of(TableInOutExchangeState.STATUS_KEY, TableInOutExchangeState.CONTINUE));
    }

    private static Reply send(RpcStream<?> stream, Map<String, String> metadata, long... values) {
        try (VectorSchemaRoot in = rows(values)) {
            // The reply's root belongs to the session (its reader reuses it).
            AnnotatedBatch reply = stream.exchange(new AnnotatedBatch(in, metadata));
            VectorSchemaRoot root = reply.root();
            BigIntVector n = (BigIntVector) root.getVector("n");
            List<Long> out = new ArrayList<>();
            for (int i = 0; i < root.getRowCount(); i++) out.add(n.get(i));
            return new Reply(out, TableInOutExchangeState.saysMore(reply.customMetadata()));
        }
    }

    private static VectorSchemaRoot rows(long... values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
        root.allocateNew();
        BigIntVector n = (BigIntVector) root.getVector("n");
        for (int i = 0; i < values.length; i++) n.setSafe(i, values[i]);
        n.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    private static String failure(Runnable turn) {
        RuntimeException e = assertThrows(RuntimeException.class, turn::run);
        return String.valueOf(e.getMessage());
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(Transport.class)
    void continuesAcrossTicks(Transport t) throws Exception {
        VgiService vgi = connect(t);
        RpcStream<?> stream = open(vgi, "pages");
        try {
            assertEquals(new Reply(List.of(1L, 2L), true), send(stream, 1, 2, 3, 4, 5));
            // Over HTTP this tick is its own request: its vgi.status must reach
            // the exchange, and the pending flag must come back in the token.
            assertEquals(new Reply(List.of(3L, 4L), true), tick(stream));
            assertEquals(new Reply(List.of(5L), false), tick(stream));
            // Output done: real input is valid again on the same stream.
            assertEquals(new Reply(List.of(7L, 8L), false), send(stream, 7, 8));
        } finally {
            stream.close();
        }
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void answersAZeroRowInputWithNothingPending(Transport t) throws Exception {
        VgiService vgi = connect(t);
        RpcStream<?> stream = open(vgi, "pages");
        try {
            assertEquals(new Reply(List.of(), false), send(stream));
            assertEquals(new Reply(List.of(9L), false), send(stream, 9));
        } finally {
            stream.close();
        }
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void refusesATickAnsweredWithNoBatch(Transport t) throws Exception {
        VgiService vgi = connect(t);
        RpcStream<?> stream = open(vgi, "silent_resume");
        try {
            assertEquals(new Reply(List.of(1L), true), send(stream, 1));
            String msg = failure(() -> tick(stream));
            assertTrue(msg.contains("no data batch was emitted"), msg);
        } finally {
            stream.close();
        }
        assertUsable(vgi);
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void refusesAnInputAnsweredWithNoBatch(Transport t) throws Exception {
        VgiService vgi = connect(t);
        RpcStream<?> stream = open(vgi, "silent_input");
        try {
            String msg = failure(() -> send(stream, 1, 2));
            assertTrue(msg.contains("no data batch was emitted"), msg);
        } finally {
            stream.close();
        }
        assertUsable(vgi);
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void failsTheStreamOnAResumeError(Transport t) throws Exception {
        VgiService vgi = connect(t);
        RpcStream<?> stream = open(vgi, "failing_resume");
        try {
            assertEquals(new Reply(List.of(1L), true), send(stream, 1));
            String msg = failure(() -> tick(stream));
            assertTrue(msg.contains("failing on page 2 as asked"), msg);
        } finally {
            stream.close();
        }
        assertUsable(vgi);
    }

    /** The connection serves a fresh stream after a failed one. */
    private static void assertUsable(VgiService vgi) {
        RpcStream<?> again = open(vgi, "pages");
        try {
            Reply r = send(again, 4, 5, 6);
            assertEquals(new Reply(List.of(4L, 5L), true), r);
            assertEquals(new Reply(List.of(6L), false), tick(again));
        } finally {
            again.close();
        }
        assertFalse(vgi.catalog_schemas(
                vgi.catalog_attach(CatalogAttachRequest.of("cont", null, null, null), null).attach_opaque_data(),
                null, null).items().isEmpty());
    }
}
