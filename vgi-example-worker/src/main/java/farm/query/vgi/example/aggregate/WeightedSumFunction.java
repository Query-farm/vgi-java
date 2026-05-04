// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.ScalarHelpers;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
            new Field("result", new FieldType(true, Schemas.FLOAT64, null), null)));

    @Override public String name() { return "vgi_weighted_sum"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Weighted sum (value * weight)");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("value", 0, Schemas.FLOAT64),
                new ArgSpec("weight", 1, Schemas.FLOAT64));
    }
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
    public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
        ((Float8Vector) output.getVector("result")).setSafe(rowIndex, state.total);
    }
}
