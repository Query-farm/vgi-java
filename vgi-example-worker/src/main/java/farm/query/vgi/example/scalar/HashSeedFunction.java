// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * {@code hash_seed(seed: int64 [const]) -> int64}: generates {@code seed + row_index}
 * for each row of the input batch. Demonstrates a function with const-only args
 * that emits values driven by row count rather than input columns.
 */
public final class HashSeedFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.INT64);

    private static final FunctionSpec SPEC = FunctionSpec.builder("hash_seed")
            .description("Generate deterministic integers from a constant seed")
            .constArg("seed", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        long seed = ParameterExtractor.of(params.arguments())
                .positional(0, "seed").asLong().required();
        return ScalarHelpers.mapInt64Raw(Schemas.singleResult(Schemas.INT64), input, alloc,
                row -> seed + row);
    }
}
