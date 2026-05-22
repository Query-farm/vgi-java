// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
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

    private static final FunctionSpec SPEC = FunctionSpec.builder("conditional_message")
            .description("Returns repeated message when condition is true")
            .constArg("repeat_count", Schemas.INT64)
            .constArg("message", Schemas.UTF8)
            .arg("condition", Schemas.BOOL)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

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
