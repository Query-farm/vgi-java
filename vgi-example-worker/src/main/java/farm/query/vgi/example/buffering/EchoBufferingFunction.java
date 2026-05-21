// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.buffering;

import farm.query.vgi.function.FunctionMetadata;

/**
 * {@code echo_buffering(data TABLE) -> *} — buffered passthrough with projection
 * + filter pushdown. The framework narrows the output schema and applies pushed
 * filters; the fixture stays a plain buffer-and-drain. Mirrors vgi-python
 * {@code EchoBufferingFunction}.
 */
public final class EchoBufferingFunction extends AbstractBufferAndDrain {

    @Override public String name() { return "echo_buffering"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Buffered passthrough with projection + filter pushdown")
                .withPushdown(true, true, true)
                .withCategories("test", "buffer", "pushdown");
    }
}
