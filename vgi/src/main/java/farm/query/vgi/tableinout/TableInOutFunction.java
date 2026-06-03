// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.protocol.BindResponse;


/**
 * A VGI table-in-out function: receives input batches and emits output batches,
 * one output batch per input batch (echo, filter, transform). State may
 * accumulate across exchange ticks and round-trips with the stream, so it works
 * on both the launcher and HTTP transports.
 *
 * <p>Functions that need a Sink+Source shape — buffer the whole input, then
 * emit a summary at the end (sums, distributed aggregation, full buffering) —
 * use {@link farm.query.vgi.buffering.TableBufferingFunction} instead, whose
 * worker-side storage (keyed by {@code execution_id}) survives the stateless
 * HTTP process→combine→finalize round-trip. The old TIO finalize phase relied
 * on an in-process exchange-state map and could not work over stateless HTTP,
 * so it was removed.
 *
 * <p>Mirrors {@code vgi.TableInOutFunction} in vgi-go.
 */
public interface TableInOutFunction extends FunctionDescriptor {

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
