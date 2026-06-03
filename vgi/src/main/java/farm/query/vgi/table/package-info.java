// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Read-only table-producing VGI functions and their bind/scan parameter and result types.
 *
 * <p>A {@link farm.query.vgi.table.TableFunction} generates a stream of Arrow
 * batches with no table input. The framework binds the call (producing an
 * output schema), constructs a per-execution producer, then drives that
 * producer tick-by-tick until end-of-stream — passing pushdown filters,
 * projection, ORDER BY, and sampling hints from DuckDB's optimiser.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.table.TableFunction} — the table-function contract
 *       ({@code onBind}, {@code createProducer}, plus optimiser hooks).</li>
 *   <li>{@link farm.query.vgi.table.TableBindParams} — bind-time parameters.</li>
 *   <li>{@link farm.query.vgi.table.TableInitParams} — per-execution init parameters
 *       carrying pushdown, projection, and sampling hints.</li>
 *   <li>{@link farm.query.vgi.table.TableProducerState} — base for per-execution
 *       producers that emit one batch per tick.</li>
 *   <li>{@link farm.query.vgi.table.SimpleTableFunction} and
 *       {@link farm.query.vgi.table.CountdownTableFunction} — fixed-schema and
 *       sequence-like base classes that supply common scaffolding.</li>
 * </ul>
 */
package farm.query.vgi.table;
