// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/** {@code multiply(value INT64, factor INT64 [const]) -> INT64}. */
public final class MultiplyFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.INT64);

    @Override public String name() { return "multiply"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Multiplies a value by a constant factor");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("value", 0, Schemas.INT64),
                ArgSpec.positional("factor", 1, Schemas.INT64));
    }
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
