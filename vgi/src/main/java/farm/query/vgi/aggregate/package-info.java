// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Batch-oriented aggregate user-defined functions for the VGI protocol.
 *
 * <p>An aggregate folds many input rows into one result row per group. State is
 * per-group and accumulates across {@code update} calls, merges across parallel
 * workers via {@code combine}, and is serialized over the wire so partial
 * accumulators can cross process boundaries before {@code finalize} emits each
 * group's single output column.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.aggregate.AggregateFunction} — the aggregate UDF
 *       interface: {@code newState}/{@code update}/{@code combine}/{@code finalize}
 *       lifecycle plus state (de)serialization for cross-process accumulators.</li>
 * </ul>
 */
package farm.query.vgi.aggregate;
