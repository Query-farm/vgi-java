// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Catalog metadata descriptors a VGI worker exposes to DuckDB.
 *
 * <p>These immutable records model the schema-level objects (tables, views,
 * macros) and their inline metadata (column statistics, scan branches) that the
 * C++ extension reads while resolving and planning queries against an attached
 * worker. Their wire shapes are byte-compatible with the canonical Python and Go
 * implementations.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.catalog.CatalogTable} — a catalog table with its
 *       Arrow schema, backing scan function, constraints and statistics.</li>
 *   <li>{@link farm.query.vgi.catalog.View} — a SQL view defined by a query string.</li>
 *   <li>{@link farm.query.vgi.catalog.Macro} — a scalar or table SQL macro.</li>
 *   <li>{@link farm.query.vgi.catalog.ColumnStatistics} — per-column min/max and
 *       null/distinct metadata used by the optimizer.</li>
 *   <li>{@link farm.query.vgi.catalog.ScanBranch} — one physical source backing a
 *       multi-branch table.</li>
 * </ul>
 */
package farm.query.vgi.catalog;
