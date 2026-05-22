// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.ScalarHelpers;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** {@code vgi_weighted_sum(value: double, weight: double) -> double}. */
public final class WeightedSumFunction implements AggregateFunction<WeightedSumFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        double total;
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("result", Schemas.FLOAT64)));

    private static final FunctionSpec SPEC = FunctionSpec.builder("vgi_weighted_sum")
            .description("Weighted sum of values")
            .arg("value", Schemas.FLOAT64)
            .arg("weight", Schemas.FLOAT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        FieldVector v = input.getFieldVectors().get(0);
        FieldVector w = input.getFieldVectors().get(1);
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            if (v.isNull(i) || w.isNull(i)) continue;
            State s = states.computeIfAbsent(groupIds[i], k -> new State());
            s.total += ScalarHelpers.toDouble(v, i) * ScalarHelpers.toDouble(w, i);
        }
    }

    @Override
    public void combine(State target, State source) { target.total += source.total; }

    @Override
    public void finalize(FieldVector result, int rowIndex, State state) {
        ((Float8Vector) result).setSafe(rowIndex, state.total);
    }
}
