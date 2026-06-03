// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;

/**
 * {@code ordered_buffer_input} — buffer_input variant with single-threaded
 * ingest ({@code sink_order_dependent=true} → C++ {@code ParallelSink=false}).
 */
public final class OrderedBufferInputFunction extends AbstractBufferAndDrain {

    private static final FunctionSpec SPEC = FunctionSpec.builder("ordered_buffer_input")
            .metadata(FunctionMetadata.describe("buffer_input variant with sink_order_dependent=True")
                    .withCategories("test", "ordering"))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public boolean sinkOrderDependent() { return true; }
}
