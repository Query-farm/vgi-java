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

/**
 * {@code multiply_by_setting(value: int64) -> int64}: multiplies the input
 * column by the {@code multiplier} session setting. Demonstrates settings
 * propagation: the setting value arrives in {@link ScalarBindParams#settings}
 * and is captured into {@link ScalarProcessParams#settings} for use at
 * exchange time.
 */
public final class MultiplyBySettingFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.INT64);

    @Override public String name() { return "multiply_by_setting"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Multiplies the input by the 'multiplier' setting");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec("value", 0, Schemas.INT64));
    }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        Object setting = params.settings() == null ? null : params.settings().get("multiplier");
        long multiplier = (setting instanceof Number n) ? n.longValue() : 1L;
        FieldVector value = input.getFieldVectors().get(0);
        return ScalarHelpers.mapInt64(Schemas.singleResult(Schemas.INT64), input, alloc, value,
                row -> ScalarHelpers.toLong(value, row) * multiplier);
    }
}
