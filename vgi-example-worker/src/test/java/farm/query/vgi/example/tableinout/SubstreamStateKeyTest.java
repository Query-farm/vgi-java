// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.example.Main;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Table-in-out state is keyed per substream, not per process.
 *
 * <p>A table-in-out function with a finalize upserts its running state into execution-scoped
 * storage after every batch, and {@code finish()} drains every row the execution stored. The
 * storage is already scoped to the execution, and every connection of a fanned-out scan shares
 * that execution -- so the row key has to tell those connections apart. Keyed by the process id,
 * it only did when each connection was its own process. Under the launcher, TCP or threaded HTTP
 * one process serves many connections: they overwrote each other's row and {@code finish()}
 * undercounted (vgi-python measured {@code substream_partial_sum} at 194850 for 1999000). The key
 * is now {@code InitRequest.substream_id}, the client-minted id the DuckDB extension already sends
 * for every substream, and the pid only when a client sent none. Mirrors vgi-python 072e543.
 *
 * <p>Driven through the real example worker, one process and one storage serving several client
 * connections at once, over two transports: pipes, each served on its own thread by one
 * {@link RpcServer} (the shape of a launcher-spawned or TCP worker), and one {@link HttpServer}
 * (threaded HTTP, where the exchange state -- and so the key it carries -- rides a continuation
 * token between requests).
 */
@Timeout(60)
final class SubstreamStateKeyTest {

    private static final Schema INPUT = Schemas.of(Schemas.nullable("n", Schemas.INT64));
    private static final SecureRandom RANDOM = new SecureRandom();

    /** How the several connections reach the one worker process. */
    enum Transport { PIPE, HTTP }

    private final List<AutoCloseable> connections = new ArrayList<>();
    private RpcServer server;
    private HttpServer http;
    private Transport transport;

    @BeforeEach
    void startWorker() {
        Worker worker = Main.buildWorker("example", null, null);
        server = worker.rpcServer();
    }

    @AfterEach
    void stopWorker() throws Exception {
        for (AutoCloseable c : connections) c.close();
        if (http != null) http.stop();
    }

    // ------------------------------------------------------------------
    // The fix
    // ------------------------------------------------------------------

    /**
     * Two substreams of one execution, served by one process, both reach {@code finish()}.
     *
     * <p>Under the per-process key the second connection's row replaced the first's, and the
     * partial came back as the second stream's sum alone.
     */
    @ParameterizedTest
    @EnumSource(Transport.class)
    void twoSubstreamsInOneProcessBothReachFinish(Transport t) throws Exception {
        transport = t;
        long[] first = LongStream.rangeClosed(1, 100).toArray();       // 5050
        long[] second = LongStream.rangeClosed(1_000, 1_049).toArray(); // 51225
        List<List<Long>> finalized = runExecution("substream_partial_sum",
                List.of(new Substream(newSubstreamId(), first), new Substream(newSubstreamId(), second)));
        assertEquals(List.of(List.of(sum(first) + sum(second))), finalized);
    }

    /**
     * The same through {@code multi_batch_finish}, whose finish emits one batch per row seen.
     *
     * <p>A row count the undercount cannot hide: a lost substream shows up as missing batches as
     * well as a wrong total.
     */
    @ParameterizedTest
    @EnumSource(Transport.class)
    void twoSubstreamsInOneProcessBothReachAMultiBatchFinish(Transport t) throws Exception {
        transport = t;
        long[] first = {1, 2, 3};
        long[] second = {10, 20, 30, 40};
        List<List<Long>> finalized = runExecution("multi_batch_finish",
                List.of(new Substream(newSubstreamId(), first), new Substream(newSubstreamId(), second)));
        List<List<Long>> expected = new ArrayList<>();
        expected.add(List.of(sum(first) + sum(second)));
        for (int i = 1; i < first.length + second.length; i++) expected.add(List.of(0L));
        assertEquals(expected, finalized);
    }

    /**
     * A client that sends no {@code substream_id} is keyed by process, as before, and its state
     * still reaches {@code finish()}.
     *
     * <p>The pid remains only for such a client. Every connection of an old client that fans out
     * over processes -- subprocess workers -- is its own process, so that key still separates them.
     */
    @ParameterizedTest
    @EnumSource(Transport.class)
    void aClientSendingNoSubstreamIdStillReachesFinish(Transport t) throws Exception {
        transport = t;
        long[] only = LongStream.rangeClosed(1, 2_000).toArray();
        assertEquals(List.of(List.of(sum(only))),
                runExecution("substream_partial_sum", List.of(new Substream(null, only))));
    }

    // ------------------------------------------------------------------
    // One execution, several connections
    // ------------------------------------------------------------------

    private record Substream(byte[] id, long[] values) { }

    /**
     * Drive one execution the way a fanned-out client does: every substream's INPUT stream on its
     * own connection, sharing the {@code execution_id} the first init minted, then one FINALIZE on
     * the first connection carrying the first substream's id.
     *
     * @return the finalize output, one list per emitted batch
     */
    private List<List<Long>> runExecution(String function, List<Substream> substreams) throws Exception {
        List<VgiService> clients = new ArrayList<>();
        List<byte[]> handles = new ArrayList<>();
        for (int i = 0; i < substreams.size(); i++) {
            VgiService client = connect();
            clients.add(client);
            handles.add(client.catalog_attach(CatalogAttachRequest.of("example", null, null, null), null)
                    .attach_opaque_data());
        }

        byte[] executionId = null;
        for (int i = 0; i < substreams.size(); i++) {
            BindRequest bind = bind(function, handles.get(i));
            BindResponse bound = clients.get(i).bind(bind, null);
            RpcStream<? extends StreamState> stream = clients.get(i).init(
                    init(bind, bound, "INPUT", executionId, substreams.get(i).id()), null);
            if (executionId == null) {
                executionId = ((GlobalInitResponse) stream.header()).execution_id();
            }
            RpcStream<? extends StreamState> session = stream;
            try {
                // Two batches, so the upsert replaces a row this substream already wrote.
                long[] values = substreams.get(i).values();
                int half = values.length / 2;
                exchange(session, java.util.Arrays.copyOfRange(values, 0, half));
                exchange(session, java.util.Arrays.copyOfRange(values, half, values.length));
            } finally {
                session.close();
            }
        }

        BindRequest bind = bind(function, handles.get(0));
        BindResponse bound = clients.get(0).bind(bind, null);
        RpcStream<? extends StreamState> finalize = clients.get(0).init(
                init(bind, bound, "FINALIZE", executionId, substreams.get(0).id()), null);
        return drain(finalize);
    }

    private static BindRequest bind(String function, byte[] handle) {
        return new BindRequest(
                function,
                null,                                   // arguments: the TABLE argument is the stream
                "TABLE",
                SchemaUtil.serializeSchema(INPUT),      // input_schema: a table-in-out needs one
                null, null,                             // settings, secrets
                handle,
                null,                                   // transaction_opaque_data
                false,
                null, null,                             // at_unit / at_value
                null, null,                             // copy_from / copy_to
                "main");
    }

    private static InitRequest init(BindRequest bind, BindResponse bound, String phase,
                                    byte[] executionId, byte[] substreamId) {
        return new InitRequest(
                RecordCodec.serializeToBytes(bind),
                bound.output_schema(),
                bound.opaque_data(),
                null, null, null,                       // projection_ids, pushdown_filters, join_keys
                phase,
                executionId,                            // null on the first init: the worker mints it
                null,                                   // init_opaque_data
                null, null, null, null,                 // order-by hint
                null, null,                             // tablesample hint
                null,                                   // finalize_state_id
                substreamId,
                null,                                   // split_tokens
                null);                                  // row_limit
    }

    private static void exchange(RpcStream<?> session, long[] values) {
        try (VectorSchemaRoot input = VectorSchemaRoot.create(INPUT, Allocators.root())) {
            input.allocateNew();
            BigIntVector n = (BigIntVector) input.getVector("n");
            for (int i = 0; i < values.length; i++) n.setSafe(i, values[i]);
            n.setValueCount(values.length);
            input.setRowCount(values.length);
            // A table-in-out with a finalize accumulates: each answer is an empty batch.
            assertEquals(0, session.exchange(new AnnotatedBatch(input, null)).root().getRowCount());
        }
    }

    private static List<List<Long>> drain(RpcStream<?> session) {
        List<List<Long>> batches = new ArrayList<>();
        try {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = session.tick();
                } catch (NoSuchElementException endOfStream) {
                    break;
                }
                VectorSchemaRoot root = batch.root();
                BigIntVector n = (BigIntVector) root.getVector(0);
                List<Long> rows = new ArrayList<>();
                for (int i = 0; i < root.getRowCount(); i++) rows.add(n.get(i));
                if (!rows.isEmpty()) batches.add(rows);
            }
        } finally {
            session.close();
        }
        return batches;
    }

    private static byte[] newSubstreamId() {
        byte[] id = new byte[16];
        RANDOM.nextBytes(id);
        return id;
    }

    private static long sum(long[] values) {
        return LongStream.of(values).sum();
    }

    // ------------------------------------------------------------------
    // A client connection to the one server
    // ------------------------------------------------------------------

    private VgiService connect() throws Exception {
        if (transport == Transport.HTTP) {
            if (http == null) {
                http = new HttpServer(server, HttpServer.Config.builder().host("127.0.0.1").port(0).build());
                http.start();
            }
            HttpRpcConnection c = HttpRpcConnection.builder("http://127.0.0.1:" + http.port()).build();
            connections.add(c::close);
            return c.proxy(VgiService.class);
        }
        Connection c = new Connection(server);
        connections.add(c::close);
        return c.proxy;
    }

    /** One pipe pair, served on its own thread by the shared server. */
    private static final class Connection {
        final VgiService proxy;
        private final RpcConnection rpc;
        private final PipeTransport client;
        private final Thread serverThread;

        Connection(RpcServer server) throws IOException {
            PipedOutputStream clientOut = new PipedOutputStream();
            PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 20);
            PipedOutputStream serverOut = new PipedOutputStream();
            PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 20);
            PipeTransport serverSide = new PipeTransport(serverIn, serverOut);
            client = new PipeTransport(clientIn, clientOut);
            serverThread = new Thread(() -> server.serve(serverSide), "vgi-worker-connection");
            serverThread.setDaemon(true);
            serverThread.start();
            rpc = new RpcConnection(client);
            proxy = rpc.proxy(VgiService.class);
        }

        void close() throws InterruptedException {
            rpc.close();
            client.close();
            serverThread.join(5_000);
        }
    }

    /** Non-owning transport over an existing stream pair. */
    private record PipeTransport(InputStream reader, OutputStream writer) implements RpcTransport {
        @Override public void close() {
            try { writer.flush(); } catch (Exception ignore) { /* best-effort */ }
            try { writer.close(); } catch (Exception ignore) { /* best-effort */ }
            try { reader.close(); } catch (Exception ignore) { /* best-effort */ }
        }
    }
}
