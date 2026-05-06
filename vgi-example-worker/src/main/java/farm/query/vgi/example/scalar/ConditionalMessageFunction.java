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
 * {@code conditional_message(repeat_count BIGINT [const], message VARCHAR
 * [const], condition BOOLEAN) -> VARCHAR}.
 *
 * <p>Returns {@code message} repeated {@code repeat_count} times when
 * {@code condition} is true, otherwise an empty string. Exercises the const-arg
 * path: {@code repeat_count} and {@code message} are pulled from
 * {@link ScalarProcessParams#arguments()} (parsed once at bind time);
 * {@code condition} arrives per-row in the input batch.
 */
public final class ConditionalMessageFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.UTF8);

    @Override public String name() { return "conditional_message"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Returns repeated message when condition is true");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("repeat_count", 0, Schemas.INT64, /*isConst=*/true),
                new ArgSpec("message", 1, Schemas.UTF8, /*isConst=*/true),
                new ArgSpec("condition", 2, Schemas.BOOL));
    }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        long repeat = ((Number) params.arguments().positionalAt(0)).longValue();
        String message = (String) params.arguments().positionalAt(1);
        String repeated = (repeat <= 0 || message == null) ? "" : message.repeat((int) repeat);

        FieldVector condition = input.getFieldVectors().get(0);
        return ScalarHelpers.mapString(Schemas.singleResult(Schemas.UTF8), input, alloc, null,
                row -> ScalarHelpers.toBool(condition, row) ? repeated : "");
    }
}
