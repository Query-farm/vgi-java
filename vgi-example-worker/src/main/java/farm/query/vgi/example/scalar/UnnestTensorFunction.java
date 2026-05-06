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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

/**
 * Stub for {@code unnest_tensor(value ANY)} — placeholder so the catalog
 * exposes the name. Returns NULLs; tensor decoding is not yet implemented.
 */
public final class UnnestTensorFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.FLOAT64);

    @Override public String name() { return "unnest_tensor"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Unnest a tensor value into rows (stub)");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec("value", 0, new ArrowType.Null(), "", false, false, "",
                List.of(), false, true));
    }
    @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUTPUT_SCHEMA_IPC); }
    @Override public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input,
                                                 BufferAllocator alloc) {
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        for (int i = 0; i < rows; i++) out.getVector("result").setNull(i);
        out.setRowCount(rows);
        return out;
    }
}
