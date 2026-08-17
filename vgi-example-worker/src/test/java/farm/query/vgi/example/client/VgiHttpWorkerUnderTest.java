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

    /**
     * How this worker spells a table function in
     * {@code FunctionInfo.function_type}.
     *
     * <p>It is not the same on both sides, and neither is wrong. vgi-python
     * puts enum <em>names</em> on the wire throughout — {@code TABLE},
     * {@code PRESERVES_ORDER}, {@code NOT_PARTITIONED} — while vgi-java sends
     * names for every one of those except this field, where it sends the enum
     * <em>value</em> {@code table}. The C++ extension, which is the only
     * normative reader of the field, accepts both spellings on purpose
     * ({@code ParseVgiFunctionType} in {@code vgi_catalog_api.cpp} matches
     * {@code "table" || "TABLE" || "table_in_out"}), so the protocol genuinely
     * does not pin the case here and both workers are conformant.
     *
     * <p>This is declared per leg rather than normalised away in the shared
     * assertion, so the divergence stays visible in the source and a change on
     * either side fails a test instead of passing through a case-insensitive
     * comparison. A JVM consumer switching on this string must accept both.
     *
     * @return the {@code function_type} value this worker emits for a table function
     */
    String tableFunctionTypeLabel();

    @Override
    void close();
}
