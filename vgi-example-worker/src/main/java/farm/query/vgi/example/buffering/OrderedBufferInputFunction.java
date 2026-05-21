// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.buffering;

import farm.query.vgi.function.FunctionMetadata;

/**
 * {@code ordered_buffer_input} — buffer_input variant with single-threaded
 * ingest ({@code sink_order_dependent=true} → C++ {@code ParallelSink=false}).
 */
public final class OrderedBufferInputFunction extends AbstractBufferAndDrain {

    @Override public String name() { return "ordered_buffer_input"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("buffer_input variant with sink_order_dependent=True")
                .withCategories("test", "ordering");
    }

    @Override public boolean sinkOrderDependent() { return true; }
}
