// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stub aggregates — exposed in the catalog so function_registration tests
 * can verify the expected name set. Their compute paths fall back to
 * passthrough sums or NULL outputs; tests that exercise the actual
 * window-aggregate or nest_tensor semantics still fail until those
 * subsystems are implemented.
 */
public final class StubAggregates {

    private StubAggregates() {}

    /** {@code vgi_streaming_sum(value BIGINT) -> BIGINT}. */
    public static final class StreamingSum implements AggregateFunction<StreamingSum.State> {
        public static final class State implements Serializable {
            private static final long serialVersionUID = 1L;
            long sum;
        }
        @Override public String name() { return "vgi_streaming_sum"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("Streaming sum (stub)"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("value", 0, Schemas.INT64));
        }
        @Override public Schema outputSchema() { return Schemas.singleResult(Schemas.INT64); }
        @Override public State newState() { return new State(); }
        @Override public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
            FieldVector v = input.getFieldVectors().get(0);
            int rows = input.getRowCount();
            for (int i = 0; i < rows; i++) {
                State s = states.computeIfAbsent(groupIds[i], k -> new State());
                if (!v.isNull(i)) s.sum += farm.query.vgi.types.ScalarHelpers.toLong(v, i);
            }
        }
        @Override public void combine(State target, State source) { target.sum += source.sum; }
        @Override public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
            ((BigIntVector) output.getVector("result")).setSafe(rowIndex, state.sum);
        }
        @Override public void finalizeEmpty(VectorSchemaRoot output, int rowIndex) {
            ((BigIntVector) output.getVector("result")).setSafe(rowIndex, 0);
        }
    }
    /** Window-flavoured stubs — registered to keep the function_registration count happy. */
    public abstract static class WindowStub implements AggregateFunction<WindowStub.State> {
        public static final class State implements Serializable {
            private static final long serialVersionUID = 1L;
            long sum;
            ArrayList<Double> dvals = new ArrayList<>();
            ArrayList<String> svals = new ArrayList<>();
        }
        private final String name;
        private final ArrowType outType;

        WindowStub(String name, ArrowType outType) { this.name = name; this.outType = outType; }

        @Override public String name() { return name; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe(name + " (stub)"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("value", 0, outType instanceof ArrowType.Utf8 ? Schemas.UTF8
                    : (outType instanceof ArrowType.FloatingPoint ? Schemas.FLOAT64 : Schemas.INT64)));
        }
        @Override public Schema outputSchema() { return Schemas.singleResult(outType); }
        @Override public State newState() { return new State(); }
        @Override public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
            FieldVector v = input.getFieldVectors().get(0);
            int rows = input.getRowCount();
            for (int i = 0; i < rows; i++) {
                State s = states.computeIfAbsent(groupIds[i], k -> new State());
                if (v.isNull(i)) continue;
                if (outType instanceof ArrowType.Utf8) {
                    s.svals.add(v instanceof VarCharVector vc ? vc.getObject(i).toString() : v.toString());
                } else if (outType instanceof ArrowType.FloatingPoint) {
                    s.dvals.add(farm.query.vgi.types.ScalarHelpers.toDouble(v, i));
                } else {
                    s.sum += farm.query.vgi.types.ScalarHelpers.toLong(v, i);
                }
            }
        }
        @Override public void combine(State target, State source) {
            target.sum += source.sum;
            target.dvals.addAll(source.dvals);
            target.svals.addAll(source.svals);
        }
        @Override public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
            FieldVector r = output.getVector("result");
            if (r instanceof BigIntVector bi) bi.setSafe(rowIndex, state.sum);
            else if (r instanceof Float8Vector f) {
                if (state.dvals.isEmpty()) f.setNull(rowIndex);
                else { double s = 0; for (double d : state.dvals) s += d; f.setSafe(rowIndex, s / state.dvals.size()); }
            }
            else if (r instanceof VarCharVector vc) {
                vc.setSafe(rowIndex, new Text(String.join(",", state.svals)));
            }
        }
    }

    public static final class WindowSum extends WindowStub {
        public WindowSum() { super("vgi_window_sum", Schemas.INT64); }
    }
    public static final class WindowSumBatch extends WindowStub {
        public WindowSumBatch() { super("vgi_window_sum_batch", Schemas.INT64); }
    }
    public static final class WindowMedian extends WindowStub {
        public WindowMedian() { super("vgi_window_median", Schemas.FLOAT64); }
    }
    public static final class WindowListagg extends WindowStub {
        public WindowListagg() { super("vgi_window_listagg", Schemas.UTF8); }
    }
}
