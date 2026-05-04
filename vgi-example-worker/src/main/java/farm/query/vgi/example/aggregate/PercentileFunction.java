// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@code vgi_percentile(value: DOUBLE, p: DOUBLE [const]) -> DOUBLE} —
 * approximate percentile via collected samples.
 */
public final class PercentileFunction implements AggregateFunction<PercentileFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        ArrayList<Double> values = new ArrayList<>();
        double pct = 0.5;
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("result", new FieldType(true, Schemas.FLOAT64, null), null)));

    @Override public String name() { return "vgi_percentile"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Approximate percentile using collected samples");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("value", 0, Schemas.FLOAT64),
                new ArgSpec("p", 1, Schemas.FLOAT64, /*isConst=*/true));
    }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        // Input columns: value at idx 0; the const `p` is delivered via
        // Arguments, but the aggregate framework doesn't currently thread it
        // through update/finalize. Stash any per-row p value seen in the input
        // batch (DuckDB sends const params as a column with one-row default).
        FieldVector value = input.getFieldVectors().get(0);
        FieldVector pCol = input.getFieldVectors().size() > 1 ? input.getFieldVectors().get(1) : null;
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            State s = states.computeIfAbsent(groupIds[i], k -> new State());
            if (pCol != null && !pCol.isNull(i)) s.pct = ScalarHelpers.toDouble(pCol, i);
            if (value.isNull(i)) continue;
            s.values.add(ScalarHelpers.toDouble(value, i));
        }
    }

    @Override
    public void combine(State target, State source) {
        target.values.addAll(source.values);
        if (target.pct == 0.5 && source.pct != 0.5) target.pct = source.pct;
    }

    @Override
    public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
        Float8Vector v = (Float8Vector) output.getVector("result");
        if (state.values.isEmpty()) { v.setNull(rowIndex); return; }
        ArrayList<Double> sorted = new ArrayList<>(state.values);
        Collections.sort(sorted);
        int idx = (int) (state.pct * sorted.size());
        if (idx < 0) idx = 0;
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        v.setSafe(rowIndex, sorted.get(idx));
    }
}
