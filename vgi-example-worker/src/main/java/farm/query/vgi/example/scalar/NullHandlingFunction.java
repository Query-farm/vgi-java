// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.NullHandling;
import farm.query.vgi.function.Stability;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * {@code null_handling(value: int64) -> int64}: returns the input value, or
 * {@code -5000} when the input is NULL.
 *
 * <p>Uses {@link NullHandling#SPECIAL} so DuckDB delivers NULLs to the function
 * instead of short-circuiting them client-side.
 */
public final class NullHandlingFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.INT64);
    private static final FunctionMetadata META = new FunctionMetadata(
            "Returns value or -5000 if null", Stability.CONSISTENT, NullHandling.SPECIAL, false, false, false, false);

    private static final FunctionSpec SPEC = FunctionSpec.builder("null_handling")
            .metadata(META)
            .arg("value", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        FieldVector value = input.getFieldVectors().get(0);
        return ScalarHelpers.mapInt64Raw(Schemas.singleResult(Schemas.INT64), input, alloc,
                row -> value.isNull(row) ? -5000L : ScalarHelpers.toLong(value, row));
    }
}
