// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.http;

import farm.query.vgirpc.http.HttpServer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.Callback;

/**
 * Adds a VGI capability header to every response of a vgi-rpc {@link HttpServer}, alongside
 * vgi-rpc's own capability headers (including {@code OPTIONS} discovery, which runs no
 * pre-handlers).
 *
 * <p>Used for {@code VGI-Arguments-Ref-Bytes} (protocol 3.4 argument references): vgi-rpc-java
 * has no hook for an application capability header, so this wraps the server's Jetty handler
 * before it starts. TODO(vgi-rpc-java): replace with a config option for extra capability
 * headers (it would also list the header in the CORS expose set).
 */
public final class ResponseHeaders {

    private ResponseHeaders() {}

    /**
     * Set {@code name: value} on every response {@code http} sends. Call before
     * {@link HttpServer#start()}.
     *
     * @param http the server, not yet started
     * @param name the header name
     * @param value the header value
     * @throws IllegalStateException when the server's Jetty instance cannot be reached
     */
    public static void install(HttpServer http, String name, String value) {
        Server server;
        try {
            java.lang.reflect.Field f = HttpServer.class.getDeclaredField("jetty");
            f.setAccessible(true);
            server = (Server) f.get(http);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new IllegalStateException("cannot add the " + name + " header to the HTTP server", e);
        }
        Handler inner = server.getHandler();
        server.setHandler(new Handler.Wrapper(inner) {
            @Override
            public boolean handle(Request request, Response response, Callback callback) throws Exception {
                response.getHeaders().put(name, value);
                return super.handle(request, response, callback);
            }
        });
    }
}
