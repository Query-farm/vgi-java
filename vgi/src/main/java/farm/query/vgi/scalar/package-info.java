// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * The {@code ScalarFn} framework and parameter annotations for defining scalar
 * VGI user-defined functions.
 *
 * <p>Scalar functions map 1:1 over rows: each input batch produces an output
 * batch with the same row count. Subclasses of {@link farm.query.vgi.scalar.ScalarFn}
 * declare a single {@code compute()} method whose annotated parameters drive the
 * entire function spec, output schema, and per-batch dispatch — mirroring the
 * canonical Python worker's annotation-based scalar functions.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.scalar.ScalarFn} — Pythonic base class; derives spec and dispatch from {@code compute()}.</li>
 *   <li>{@link farm.query.vgi.scalar.ScalarFunction} — lower-level interface ({@code onBind}/{@code process}) for hand-written scalars.</li>
 *   <li>{@link farm.query.vgi.scalar.Vector} — marks a {@code compute()} parameter as a per-row input column.</li>
 *   <li>{@link farm.query.vgi.scalar.Const} — marks a bind-time constant positional argument.</li>
 *   <li>{@link farm.query.vgi.scalar.Setting} — pulls a value from a DuckDB session setting.</li>
 *   <li>{@link farm.query.vgi.scalar.OutputLength} — injects the batch row count for vector-less functions.</li>
 * </ul>
 */
package farm.query.vgi.scalar;
