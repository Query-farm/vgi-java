// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;

/**
 * Base for table-in-out exchange states: each input batch produces zero or
 * one output batch via {@link #process}. Subclasses override
 * {@link #onInputBatch} which receives the {@link AnnotatedBatch} input root
 * and the {@link OutputCollector} to emit on.
 */
public abstract class TableInOutExchangeState extends ExchangeState {

    /** Sole constructor; per-execution state lives in the subclass. */
    protected TableInOutExchangeState() {}

    /**
     * Final dispatch hook from the RPC layer; forwards each input batch to
     * {@link #onInputBatch}.
     *
     * @param input the annotated input batch (root plus per-batch metadata).
     * @param out the collector to emit output batches on.
     * @param ctx the call context (client logging, cancellation, etc.).
     */
    @Override
    public final void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        onInputBatch(input, out, ctx);
    }

    /**
     * Processes a single input batch, emitting zero or one output batch.
     *
     * @param input the annotated input batch (root plus per-batch metadata).
     * @param out the collector to emit output batches on; {@code emit} takes
     *     ownership of the emitted root.
     * @param ctx the call context (client logging, cancellation, etc.).
     */
    public abstract void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx);
}
