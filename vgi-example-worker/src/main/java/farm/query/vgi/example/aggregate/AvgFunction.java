// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** {@code vgi_avg(value: int64) -> double} — averages int64 values. */
public final class AvgFunction implements AggregateFunction<AvgFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        long total;
        long count;
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("result", Schemas.FLOAT64)));

    private static final FunctionSpec SPEC = FunctionSpec.builder("vgi_avg")
            .description("Average of integer values")
            .arg("value", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        FieldVector v = input.getFieldVectors().get(0);
        if (!(v instanceof BigIntVector b)) return;
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            if (b.isNull(i)) continue;
            State s = states.computeIfAbsent(groupIds[i], k -> new State());
            s.total += b.get(i);
            s.count++;
        }
    }

    @Override
    public void combine(State target, State source) {
        target.total += source.total;
        target.count += source.count;
    }

    @Override
    public void finalize(FieldVector result, int rowIndex, State state) {
        Float8Vector v = (Float8Vector) result;
        if (state.count == 0) v.setNull(rowIndex);
        else v.setSafe(rowIndex, (double) state.total / state.count);
    }
}
