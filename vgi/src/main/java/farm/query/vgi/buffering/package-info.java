// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * The table-buffering (Sink+Source) function subsystem.
 *
 * <p>A buffering function must observe every input row before producing any
 * output. It runs as a three-phase lifecycle over the VGI wire — Sink
 * ({@code process} per input batch), {@code combine} (once, end-of-input), and
 * Source ({@code createFinalizeProducer} per output stream) — selected by the
 * {@code "table_buffering"} function type. Buffered batches are stashed in a
 * per-{@code execution_id} storage view that survives worker fan-out.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.buffering.TableBufferingFunction} — the function
 *       interface implementing the process/combine/finalize lifecycle.</li>
 *   <li>{@link farm.query.vgi.storage.BoundStorage} — the per-execution,
 *       shard-pinned storage facade buffered batches are stashed in.</li>
 *   <li>{@link farm.query.vgi.buffering.BufferingFinalizeProducer} — base
 *       Source-phase producer that narrows buffered batches to the projected
 *       schema and applies pushdown filters before emit.</li>
 *   <li>{@link farm.query.vgi.buffering.TableBufferingProcessParams},
 *       {@link farm.query.vgi.buffering.TableBufferingCombineParams},
 *       {@link farm.query.vgi.buffering.TableBufferingFinalizeParams} — the
 *       per-phase context records.</li>
 * </ul>
 */
package farm.query.vgi.buffering;
