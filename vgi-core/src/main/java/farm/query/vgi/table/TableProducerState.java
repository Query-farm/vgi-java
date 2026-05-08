// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;

/**
 * Base class for table-function producer states. Subclasses implement
 * {@link #produceTick(OutputCollector, CallContext)} which is called once per
 * tick; each call must either emit one data batch via {@code out.emit(...)}
 * or call {@code out.finish()} to signal end-of-stream.
 *
 * <p>The framework also delivers per-tick {@code custom_metadata} (dynamic
 * filter updates, cancel signals) via the {@link AnnotatedBatch} input.
 * Producers that need this information override {@link
 * #produceTick(AnnotatedBatch, OutputCollector, CallContext)} instead;
 * the no-input variant is the default for fixtures that don't care.
 */
public abstract class TableProducerState extends ProducerState {

    @Override
    public final void produce(OutputCollector out, CallContext ctx) {
        produceTick(out, ctx);
    }

    @Override
    public final void produce(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        produceTick(input, out, ctx);
    }

    /** User-supplied per-tick generator. Emit one batch or call {@code out.finish()}. */
    public abstract void produceTick(OutputCollector out, CallContext ctx);

    /**
     * Per-tick generator with access to the framework's tick {@code
     * custom_metadata}. Default delegates to the no-input overload so
     * existing fixtures keep working unchanged.
     */
    public void produceTick(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        produceTick(out, ctx);
    }
}
