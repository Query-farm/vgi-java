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
 * </ul>
 *
 * <p>Nothing here opens a connection: a client drives the wire through
 * {@code RpcConnection.proxy(VgiService.class)} from vgi-rpc-java, and these
 * encoders supply the byte-array fields of the requests it sends. The
 * end-to-end shape is demonstrated by the round-trip tests in this package's
 * test sources.
 */
package farm.query.vgi.client;
