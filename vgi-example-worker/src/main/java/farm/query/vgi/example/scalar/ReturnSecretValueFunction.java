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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * Stub for {@code return_secret_value()} — emits a placeholder VARCHAR.
 * Required for function_registration coverage; the secret subsystem that
 * resolves a real secret value is not yet implemented.
 */
public final class ReturnSecretValueFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.UTF8);

    @Override public String name() { return "return_secret_value"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Return a secret's value");
    }
    @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
    @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUTPUT_SCHEMA_IPC); }
    @Override public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input,
                                                 BufferAllocator alloc) {
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        VarCharVector v = (VarCharVector) out.getVector("result");
        Text t = new Text("");
        for (int i = 0; i < rows; i++) v.setSafe(i, t);
        out.setRowCount(rows);
        return out;
    }
}
