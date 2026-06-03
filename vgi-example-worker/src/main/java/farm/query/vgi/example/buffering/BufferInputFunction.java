// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;

/**
 * {@code buffer_input(data TABLE) -> *} — collects every input batch in the
 * Sink phase and re-emits them, one per tick, in the Source phase. The
 * canonical buffer-then-emit buffering function. Mirrors vgi-python
 * {@code BufferInputFunction}.
 */
public final class BufferInputFunction extends AbstractBufferAndDrain {

    private static final FunctionSpec SPEC = FunctionSpec.builder("buffer_input")
            .metadata(FunctionMetadata.describe("Collects all input batches and emits during finalization")
                    .withCategories("utility", "buffer"))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
}
