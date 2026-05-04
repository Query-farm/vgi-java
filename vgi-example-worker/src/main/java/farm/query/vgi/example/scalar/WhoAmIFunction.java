// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

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

    @Override public String name() { return "whoami"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Return the authenticated principal name");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec("x", 0, Schemas.INT64));
    }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        return ScalarHelpers.mapString(Schemas.singleResult(Schemas.UTF8), input, alloc, null,
                row -> "anonymous");
    }
}
