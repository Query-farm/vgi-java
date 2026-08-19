// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Client-side encoders for driving a VGI worker from the JVM.
 *
 * <p>The rest of this library is a <em>worker</em> SDK: it decodes what a query
 * engine sends. A JVM <em>client</em> — Spark, Flink, Trino, a CLI — needs the
 * inverse of each of those codecs, because it has to build the payloads the
 * engine would normally build. That is what lives here; every class in this
 * package is the mirror image of a decoder elsewhere in the library, and each
 * one names its counterpart.
 *
 * <ul>
 *   <li>{@link farm.query.vgi.client.ArgumentsEncoder} — bind-time arguments
 *       ({@code BindRequest.arguments}); inverse of {@code ArgumentsParser}.</li>
 *   <li>{@link farm.query.vgi.client.SettingsEncoder} — extension settings
 *       ({@code BindRequest.settings}); inverse of {@code SettingsParser}.</li>
 *   <li>{@link farm.query.vgi.client.PushdownFiltersEncoder} — filter pushdown
 *       ({@code InitRequest.pushdown_filters} plus {@code join_keys}); inverse of
 *       {@link farm.query.vgi.pushdown.PushdownFiltersDecoder}, and a port of the
 *       C++ extension's {@code VgiSerializeFilters}. Predicates are built from
 *       {@link farm.query.vgi.client.FilterPredicate} and attached to columns
 *       named through {@link farm.query.vgi.client.ProjectedColumns}.</li>
 *   <li>{@link farm.query.vgi.client.TableFunctionRequests} /
 *       {@link farm.query.vgi.client.ColumnStatisticsDecoder} — the packed
 *       cardinality and statistics requests, and the statistics reply.</li>
 *   <li>{@link farm.query.vgi.client.ScalarValue} — the shared value model: a
 *       Java value plus the Arrow type it is written as, so that nulls and
 *       narrow integer widths stay expressible.</li>
 *   <li>{@link farm.query.vgi.client.RetryPolicy} /
 *       {@link farm.query.vgi.client.RetryingHttpClient} — the one piece of
 *       client behaviour that is not an encoder. A worker enforces
 *       {@code max_workers} by refusing redemptions with {@code 429} and a
 *       {@code Retry-After}, and that refusal is only visible below the RPC
 *       layer: by the time a call has been decoded into a VGI response, the
 *       status is gone and backpressure is indistinguishable from failure.</li>
 * </ul>
 *
 * <p>Nothing here opens a connection of its own: a client drives the wire
 * through {@code RpcConnection.proxy(VgiService.class)} from vgi-rpc-java, and
 * these encoders supply the byte-array fields of the requests it sends —
 * {@link farm.query.vgi.client.RetryingHttpClient} decorates the
 * {@code HttpClient} that connection was given rather than replacing the
 * transport. The end-to-end shape is demonstrated by the round-trip tests in
 * this package's test sources.
 */
package farm.query.vgi.client;
