// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;

/**
 * Base class for table-function producer states. Subclasses implement
 * {@link #produce(OutputCollector, CallContext)} which is called once per tick;
 * each call must either emit one data batch via {@code out.emit(...)} or call
 * {@code out.finish()} to signal end-of-stream.
 */
public abstract class TableProducerState extends ProducerState {

    @Override
    public final void produce(OutputCollector out, CallContext ctx) {
        // Forward to the user's implementation; we override produce() (final)
        // so that per-tick metadata handling (filter pushdown updates) can be
        // injected here in later phases.
        produceTick(out, ctx);
    }

    /** User-supplied per-tick generator. Emit one batch or call {@code out.finish()}. */
    public abstract void produceTick(OutputCollector out, CallContext ctx);
}
