// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.buffering;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;

/**
 * {@code echo_buffering(data TABLE) -> *} — buffered passthrough with projection
 * + filter pushdown. The framework narrows the output schema and applies pushed
 * filters; the fixture stays a plain buffer-and-drain. Mirrors vgi-python
 * {@code EchoBufferingFunction}.
 */
public final class EchoBufferingFunction extends AbstractBufferAndDrain {

    private static final FunctionSpec SPEC = FunctionSpec.builder("echo_buffering")
            .metadata(FunctionMetadata.describe("Buffered passthrough with projection + filter pushdown")
                    .withPushdown(true, true, true)
                    .withCategories("test", "buffer", "pushdown"))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
}
