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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/** {@code upper_case(value: utf8) -> utf8}. */
public final class UpperCaseFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.UTF8);

    @Override public String name() { return "upper_case"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Converts string values to uppercase");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec("value", 0, Schemas.UTF8));
    }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        FieldVector value = input.getFieldVectors().get(0);
        return ScalarHelpers.mapString(Schemas.singleResult(Schemas.UTF8), input, alloc, value,
                row -> ScalarHelpers.toString(value, row).toUpperCase());
    }
}
