// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** {@code random_int(min: int64, max: int64) -> int64}, VOLATILE. */
public final class RandomIntFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.INT64);
    private static final FunctionMetadata META = new FunctionMetadata(
            "Generate random integers", Stability.VOLATILE, NullHandling.DEFAULT, false, false, false, false);

    @Override public String name() { return "random_int"; }
    @Override public FunctionMetadata metadata() { return META; }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("min_val", 0, Schemas.INT64),
                new ArgSpec("max_val", 1, Schemas.INT64));
    }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        FieldVector minV = input.getFieldVectors().get(0);
        FieldVector maxV = input.getFieldVectors().get(1);
        return ScalarHelpers.mapInt64Raw(Schemas.singleResult(Schemas.INT64), input, alloc, row -> {
            long lo = ScalarHelpers.toLong(minV, row);
            long hi = ScalarHelpers.toLong(maxV, row);
            if (hi <= lo) return lo;
            // hi + 1 overflows for hi = Long.MAX_VALUE; nextLong requires bound > origin.
            // Use the unbounded variant in that case.
            return hi == Long.MAX_VALUE
                    ? ThreadLocalRandom.current().nextLong(lo, hi)  // [lo, hi); approximation, hi excluded
                    : ThreadLocalRandom.current().nextLong(lo, hi + 1);
        });
    }
}
