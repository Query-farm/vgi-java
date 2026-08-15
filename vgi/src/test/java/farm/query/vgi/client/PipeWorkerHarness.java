// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.transport.RpcTransport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * An in-process {@link Worker} plus a {@link VgiService} client speaking to it
 * over a pipe pair, torn down together.
 *
 * <p>Shared by the client-side round-trip tests: each of them drives the real
 * protocol (attach, bind, init, exchange/drain) rather than calling the service
 * implementation directly, because the framing between calls is exactly where
 * client-side defects live — a stream that leaves a stale EOS behind breaks the
 * <em>next</em> call, not its own.
 */
final class PipeWorkerHarness implements AutoCloseable {

    private final RpcConnection connection;
    private final VgiService proxy;
    private final RpcTransport clientTransport;
    private final Thread serverThread;

    private PipeWorkerHarness(RpcConnection connection, VgiService proxy,
                              RpcTransport clientTransport, Thread serverThread) {
        this.connection = connection;
        this.proxy = proxy;
        this.clientTransport = clientTransport;
        this.serverThread = serverThread;
    }

    /**
     * Start {@code worker} on a daemon thread and connect a client to it.
     *
     * @param worker the worker to serve
     * @return the running harness
     * @throws IOException if the pipe pair cannot be created
     */
    static PipeWorkerHarness start(Worker worker) throws IOException {
        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 20);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 20);

        RpcTransport serverTransport = new PipeTransport(serverIn, serverOut);
        RpcTransport clientTransport = new PipeTransport(clientIn, clientOut);

        RpcServer server = worker.rpcServer();
        Thread thread = new Thread(() -> server.serve(serverTransport), "vgi-worker");
        thread.setDaemon(true);
        thread.start();

        RpcConnection connection = new RpcConnection(clientTransport);
        return new PipeWorkerHarness(connection, connection.proxy(VgiService.class),
                clientTransport, thread);
    }

    /** {@return the client proxy} */
    VgiService client() {
        return proxy;
    }

    @Override
    public void close() throws InterruptedException {
        connection.close();
        clientTransport.close();
        serverThread.join(5000);
    }

    /** Non-owning transport over an existing stream pair. */
    private static final class PipeTransport implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;

        PipeTransport(InputStream in, OutputStream out) {
            this.in = in;
            this.out = out;
        }

        @Override public InputStream reader() { return in; }

        @Override public OutputStream writer() { return out; }

        @Override public void close() {
            try { out.flush(); } catch (Exception ignore) { /* best-effort */ }
            try { out.close(); } catch (Exception ignore) { /* best-effort */ }
            try { in.close(); } catch (Exception ignore) { /* best-effort */ }
        }
    }
}
