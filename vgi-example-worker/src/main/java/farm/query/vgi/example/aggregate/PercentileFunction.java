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
            Schemas.nullable("result", Schemas.FLOAT64)));

    @Override public String name() { return "vgi_percentile"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Approximate percentile using collected samples");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("value", 0, Schemas.FLOAT64),
                ArgSpec.positional("p", 1, Schemas.FLOAT64));
    }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    @Override
    public Schema bindOutputSchema(Schema inputSchema, farm.query.vgi.function.Arguments args) {
        // Reject NULL/NaN/±Inf percentile up-front with a clear error.
        Object p = null;
        for (Object v : args.positional()) {
            if (v != null) { p = v; break; }
        }
        if (args.positional().size() > 0 && args.positional().contains(null)) {
            for (Object v : args.positional()) {
                if (v == null) {
                    throw new IllegalArgumentException("percentile must not be NULL");
                }
            }
        }
        if (p instanceof Double d) {
            if (d.isNaN() || d.isInfinite()) {
                throw new IllegalArgumentException("percentile must be a finite number");
            }
            if (d < 0.0 || d > 1.0) {
                throw new IllegalArgumentException("percentile must be in [0, 1], got " + d);
            }
        } else if (p instanceof Number n) {
            double d = n.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw new IllegalArgumentException("percentile must be a finite number");
            }
            if (d < 0.0 || d > 1.0) {
                throw new IllegalArgumentException("percentile must be in [0, 1], got " + d);
            }
        } else if (p instanceof java.math.BigDecimal bd) {
            double d = bd.doubleValue();
            if (d < 0.0 || d > 1.0) {
                throw new IllegalArgumentException("percentile must be in [0, 1], got " + d);
            }
        }
        return OUTPUT_SCHEMA;
    }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        update(states, groupIds, input, farm.query.vgi.function.Arguments.empty());
    }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input,
                          farm.query.vgi.function.Arguments args) {
        FieldVector value = input.getFieldVectors().get(0);
        int rows = input.getRowCount();
        double pct = pctFromArgs(args);
        for (int i = 0; i < rows; i++) {
            State s = states.computeIfAbsent(groupIds[i], k -> new State());
            s.pct = pct;
            if (value.isNull(i)) continue;
            s.values.add(ScalarHelpers.toDouble(value, i));
        }
    }

    private static double pctFromArgs(farm.query.vgi.function.Arguments args) {
        Object p = args.named().get("p");
        if (p == null) p = args.named().get("named_p");
        if (p == null) {
            for (Object v : args.positional()) {
                if (v instanceof Number || v instanceof java.math.BigDecimal) { p = v; break; }
            }
        }
        if (p instanceof Number n) return n.doubleValue();
        if (p instanceof java.math.BigDecimal bd) return bd.doubleValue();
        return 0.5;
    }

    @Override
    public void combine(State target, State source) {
        target.values.addAll(source.values);
        if (target.pct == 0.5 && source.pct != 0.5) target.pct = source.pct;
    }

    @Override
    public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
        finalize(output, rowIndex, state, farm.query.vgi.function.Arguments.empty());
    }

    @Override
    public void finalize(VectorSchemaRoot output, int rowIndex, State state,
                            farm.query.vgi.function.Arguments args) {
        Float8Vector v = (Float8Vector) output.getVector("result");
        if (state.values.isEmpty()) { v.setNull(rowIndex); return; }
        ArrayList<Double> sorted = new ArrayList<>(state.values);
        Collections.sort(sorted);
        double pct = pctFromArgs(args);
        if (pct == 0.5 && state.pct != 0.5) pct = state.pct;
        int idx = (int) (pct * sorted.size());
        if (idx < 0) idx = 0;
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        v.setSafe(rowIndex, sorted.get(idx));
    }
}
