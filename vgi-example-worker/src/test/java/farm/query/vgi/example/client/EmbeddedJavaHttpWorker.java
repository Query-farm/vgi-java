// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import farm.query.vgi.Worker;
import farm.query.vgi.example.Main;
import farm.query.vgirpc.http.HttpServer;

/**
 * The vgi-java example worker — the same fixture set {@code vgi-example-worker
 * --http} serves — running in this JVM on an ephemeral port.
 *
 * <p>In-process rather than a spawned {@code java -cp … Main --http}: the
 * server class is identical either way ({@link Worker#runHttp} does no more
 * than construct this same {@link HttpServer}), and keeping it in-process
 * removes a JVM start, a classpath hand-off and a stdout scrape from a test
 * whose subject is the wire protocol, not process management.
 *
 * <p>One deliberate difference from {@code --http}: {@link Worker#rpcServer()}
 * is the only public way to get a server, and it disables opaque-data sealing
 * (which the HTTP entry point turns on to bind state tokens to an authenticated
 * principal). That is invisible to a client — opaque data is opaque by
 * construction, so the client's job of echoing the bytes back is unchanged —
 * and this server is anonymous-auth loopback anyway.
 */
final class EmbeddedJavaHttpWorker implements VgiHttpWorkerUnderTest {

    private final HttpServer server;
    private final String endpoint;

    private EmbeddedJavaHttpWorker(HttpServer server, String endpoint) {
        this.server = server;
        this.endpoint = endpoint;
    }

    /**
     * Start the example catalog on a loopback ephemeral port.
     *
     * @return the running worker
     * @throws Exception if Jetty cannot bind
     */
    static EmbeddedJavaHttpWorker start() throws Exception {
        Worker worker = Main.buildWorker("example", null, null);
        HttpServer server = new HttpServer(worker.rpcServer(),
                HttpServer.Config.builder().host("127.0.0.1").port(0).build());
        server.start();
        return new EmbeddedJavaHttpWorker(server, "http://127.0.0.1:" + server.port());
    }

    @Override public String endpoint() { return endpoint; }

    @Override public String label() { return "vgi-java"; }

    /** vgi-java emits the FunctionType enum's <em>value</em> here. */
    @Override public String tableFunctionTypeLabel() { return "table"; }

    @Override
    public void close() {
        try {
            server.stop();
        } catch (Exception e) {
            throw new IllegalStateException("stopping the embedded Java worker failed", e);
        }
    }
}
