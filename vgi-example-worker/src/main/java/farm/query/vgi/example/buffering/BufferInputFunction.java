// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.buffering;

import farm.query.vgi.function.FunctionMetadata;

/**
 * {@code buffer_input(data TABLE) -> *} — collects every input batch in the
 * Sink phase and re-emits them, one per tick, in the Source phase. The
 * canonical buffer-then-emit buffering function. Mirrors vgi-python
 * {@code BufferInputFunction}.
 */
public final class BufferInputFunction extends AbstractBufferAndDrain {

    @Override public String name() { return "buffer_input"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Collects all input batches and emits during finalization")
                .withCategories("utility", "buffer");
    }
}
