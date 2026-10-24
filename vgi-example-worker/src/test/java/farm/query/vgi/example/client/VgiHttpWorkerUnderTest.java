// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

/**
 * A running VGI worker reachable over HTTP, whatever language implements it.
 *
 * <p>The whole point of {@link AbstractVgiHttpConformanceTest} is that one body
 * of assertions runs against more than one worker implementation, so the thing
 * the assertions are parameterised on has to be exactly this: an endpoint and a
 * way to shut it down. Anything more specific would leak an implementation into
 * the shared code.
 */
interface VgiHttpWorkerUnderTest extends AutoCloseable {

    /**
     * The URL prefix a {@code HttpRpcConnection} hangs its method paths off,
     * e.g. {@code http://127.0.0.1:53411}.
     *
     * @return the RPC endpoint
     */
    String endpoint();

    /**
     * A short human name for the implementation, used in assertion messages so
     * a failure says which side disagreed.
     *
     * @return the implementation label
     */
    String label();

    @Override
    void close();
}
