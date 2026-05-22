// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * {@code whoami(x: int64) -> utf8}: returns the authenticated principal name,
 * or {@code "anonymous"} when there is no authenticated context.
 *
 * <p>Phase 2 doesn't surface {@code AuthContext} into ScalarProcessParams yet —
 * we always return {@code "anonymous"}, which matches the stdio transport's
 * test expectation. HTTP-mode auth wiring lands in Phase 8.
 */
public final class WhoAmIFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.UTF8);

    private static final FunctionSpec SPEC = FunctionSpec.builder("whoami")
            .description("Return the authenticated principal name.")
            .arg("x", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        return ScalarHelpers.mapString(Schemas.singleResult(Schemas.UTF8), input, alloc, null,
                row -> "anonymous");
    }
}
