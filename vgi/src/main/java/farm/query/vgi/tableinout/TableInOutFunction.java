// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.protocol.BindResponse;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;


/**
 * A VGI table-in-out function: receives input batches and emits output batches,
 * one output batch per input batch (echo, filter, transform). State may
 * accumulate across exchange ticks and round-trips with the stream, so it works
 * on both the launcher and HTTP transports.
 *
 * <p>A streaming table-in-out may additionally declare a <em>per-substream</em>
 * finalize ({@link #hasFinalize()} + {@link #finish}): under per-substream
 * worker fan-out, DuckDB runs one substream per PipelineExecutor and issues a
 * {@code FINALIZE}-phase init (same {@code execution_id} as the substream's
 * INPUT phase) after input EOS. The finalize sees only <em>this</em>
 * substream's accumulated state — coordinate it through
 * {@code params.storage()} (execution-scoped), never a global cross-substream
 * merge.
 *
 * <p>Functions that need a <em>global</em> Sink+Combine+Source shape — buffer
 * the whole input across every substream, then emit a summary at the end
 * (sums, distributed aggregation, full buffering) — use
 * {@link farm.query.vgi.buffering.TableBufferingFunction} instead, whose
 * worker-side storage (keyed by {@code execution_id}) survives the stateless
 * HTTP process→combine→finalize round-trip.
 *
 * <p>Mirrors {@code vgi.TableInOutFunction} in vgi-go.
 */
public interface TableInOutFunction extends FunctionDescriptor {

    /**
     * Maximum parallel workers advertised on the wire {@code FunctionInfo}.
     * {@code 0} (the default) keeps the C++ extension's parallel-by-default
     * per-substream worker fan-out — and with it eligibility for the
     * exchange-mode result cache; {@code 1} is the serial opt-out (a single
     * shared worker, the pre-Phase-A behaviour). Mirrors vgi-python's
     * {@code Meta.max_workers} (unset = unbounded).
     *
     * @return the advertised worker cap; {@code 0} for unbounded.
     */
    default long maxWorkers() { return 0L; }

    /**
     * Whether this function has a per-substream FINALIZE phase. When
     * {@code true} the wire {@code FunctionInfo.has_finalize} is set, the C++
     * extension keeps the substream's connection open at input EOS, and
     * {@link #finish} runs on a {@code FINALIZE}-phase init.
     *
     * @return {@code true} when {@link #finish} should run after input EOS.
     */
    default boolean hasFinalize() { return false; }

    /**
     * Produce this substream's finalize output after all of its input batches
     * were processed. Runs on a dedicated {@code FINALIZE}-phase init whose
     * {@code execution_id} equals the INPUT phase's, so
     * {@code params.storage()} sees the state the exchange accumulated.
     *
     * @param params the finalize parameters (same shape as the exchange init;
     *     {@code inputSchema} comes from the embedded bind call).
     * @return the batches to stream, in order; each is consumed by the framework.
     */
    default List<VectorSchemaRoot> finish(TableInOutInitParams params) { return List.of(); }

    /**
     * Resolves the output schema for the given bind, typically derived from the
     * input schema and arguments.
     *
     * @param params the bind-time arguments and input schema.
     * @return the bind response carrying the serialized output schema (and any
     *     bind opaque data).
     */
    BindResponse onBind(TableInOutBindParams params);

    /**
     * Creates the per-execution exchange that processes the stream, one output
     * batch per input batch.
     *
     * @param params the negotiated schemas, arguments, settings, and allocator
     *     for this execution.
     * @return a fresh exchange state for the stream.
     */
    TableInOutExchangeState createExchange(TableInOutInitParams params);
}
