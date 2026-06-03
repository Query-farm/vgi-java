// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Table-in-out functions: streaming row-in/row-out transforms.
 *
 * <p>These map to DuckDB's {@code INOUT_FUNCTION} kind, where each input batch
 * received from the engine produces zero or one output batch. State may
 * accumulate across exchange ticks and round-trips with the stream, so the same
 * function works over both the launcher and HTTP transports. Sink+Source shapes
 * that buffer the whole input belong in {@code farm.query.vgi.buffering}.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.tableinout.TableInOutFunction} — the function
 *       interface: bind an output schema, then create a per-execution exchange.</li>
 *   <li>{@link farm.query.vgi.tableinout.TableInOutExchangeState} — the
 *       per-execution exchange, one {@code onInputBatch} call per input batch.</li>
 *   <li>{@link farm.query.vgi.tableinout.PassthroughTIOFunction} — base for
 *       functions whose output schema equals their input schema (echo, filter).</li>
 *   <li>{@link farm.query.vgi.tableinout.TableInOutBindParams} — bind-time
 *       arguments and input schema.</li>
 *   <li>{@link farm.query.vgi.tableinout.TableInOutInitParams} — per-execution
 *       schemas, settings, and allocator.</li>
 * </ul>
 */
package farm.query.vgi.tableinout;
