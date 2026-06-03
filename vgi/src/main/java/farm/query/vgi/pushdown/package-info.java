// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Filter and projection pushdown descriptors passed from DuckDB into bind and scan.
 *
 * <p>When a table function opts into filter pushdown, the C++ extension serializes the
 * predicates DuckDB extracted from the query into an Arrow IPC batch (a JSON spec column
 * plus sibling value columns). This package decodes that wire form into a read-only filter
 * AST that fixtures can inspect, format, or evaluate to compact emitted batches.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.pushdown.PushdownFilters} — the parsed top-level filter list
 *       (implicit AND), with inspection, formatting, and row-evaluation helpers.</li>
 *   <li>{@link farm.query.vgi.pushdown.PushdownFilter} — the sealed predicate AST
 *       (constant, null checks, IN, AND/OR, struct recursion).</li>
 *   <li>{@link farm.query.vgi.pushdown.PushdownFiltersDecoder} — decodes the filter IPC
 *       bytes into a {@code PushdownFilters} AST.</li>
 *   <li>{@link farm.query.vgi.pushdown.FilterApplier} — convenience wrapper that decodes
 *       once per init and compacts batches on emit.</li>
 *   <li>{@link farm.query.vgi.pushdown.ComparisonOperator} — the six comparison operators a
 *       constant filter can carry.</li>
 * </ul>
 */
package farm.query.vgi.pushdown;
