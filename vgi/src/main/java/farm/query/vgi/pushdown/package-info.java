// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Filter and projection pushdown descriptors passed from DuckDB into bind and scan.
 *
 * <p>When a table function opts into filter pushdown, the C++ extension serializes the
 * predicates DuckDB extracted from the query into an Arrow IPC batch (a JSON spec column
 * plus typed sibling payload columns). This package strictly decodes Filter Encoding v2,
 * tracks snapshot/delta revisions, and evaluates its typed expression tree.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.pushdown.PushdownFilters} — the parsed top-level filter list
 *       (implicit AND), with inspection, formatting, and row-evaluation helpers.</li>
 *   <li>{@link farm.query.vgi.pushdown.FilterExpression} — the complete typed v2 AST,
 *       including recursively nested field references.</li>
 *   <li>{@link farm.query.vgi.pushdown.PushdownFilter} — a compatibility inspection view.</li>
 *   <li>{@link farm.query.vgi.pushdown.PushdownFiltersDecoder} — decodes the filter IPC
 *       bytes into a {@code PushdownFilters} AST.</li>
 *   <li>{@link farm.query.vgi.pushdown.FilterApplier} — convenience wrapper that decodes
 *       once per init and compacts batches on emit.</li>
 *   <li>{@link farm.query.vgi.pushdown.ComparisonOperator} — all v2 comparison operators.</li>
 * </ul>
 */
package farm.query.vgi.pushdown;
