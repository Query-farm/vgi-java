// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** {@code multiply(value INT64, factor INT64 [const]) -> INT64}. */
public final class MultiplyFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.INT64);

    private static final FunctionSpec SPEC = FunctionSpec.builder("multiply")
            .description("Multiplies a value by a constant factor")
            .arg("value", Schemas.INT64)
            .constArg("factor", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        long factor = ((Number) params.arguments().positionalAt(0)).longValue();
        BigIntVector value = (BigIntVector) input.getFieldVectors().get(0);
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        BigIntVector v = (BigIntVector) out.getVector("result");
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) v.setNull(i);
            else v.setSafe(i, value.get(i) * factor);
        }
        out.setRowCount(rows);
        return out;
    }
}
