// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Shared Arrow and VGI type helpers used across the public worker API.
 *
 * <p>These utilities support fixture and function implementations that build
 * Arrow {@link org.apache.arrow.vector.types.pojo.Schema}s, compute per-row
 * scalar outputs, and translate between Arrow types and DuckDB-compatible SQL
 * type names and promotion rules.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.types.Schemas} — builders for {@link org.apache.arrow.vector.types.pojo.Field}s,
 *       {@link org.apache.arrow.vector.types.pojo.Schema}s, and their IPC byte encodings.</li>
 *   <li>{@link farm.query.vgi.types.ScalarHelpers} — numeric dispatch, per-row mapping, and
 *       widening value extractors for scalar function processing.</li>
 *   <li>{@link farm.query.vgi.types.TypeRules} — DuckDB-compatible numeric promotion rules and
 *       Arrow-to-SQL type-name rendering.</li>
 *   <li>{@link farm.query.vgi.types.CachedSchema} — a wire-portable IPC-encoded schema with
 *       on-demand deserialisation caching for table-producer states.</li>
 * </ul>
 */
package farm.query.vgi.types;
