// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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

    @Override
    public final void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        onInputBatch(input, out, ctx);
    }

    public abstract void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx);
}
