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

    /** {@code nest_tensor(value ANY, axes STRUCT) -> struct<tensor, axes>} —
     *  tensor-shaping aggregate. State accumulates (value, axes-tuple) pairs;
     *  finalize sorts by axes and builds a (possibly nested) list. Supports
     *  1D and 2D tensors over BIGINT values + BIGINT axes.
     */
    public static final class NestTensor implements AggregateFunction<NestTensor.State> {
        public static final class State implements Serializable {
            private static final long serialVersionUID = 1L;
            ArrayList<Long> values = new ArrayList<>();
            ArrayList<long[]> axes = new ArrayList<>();
            int axisCount = -1;
            ArrayList<String> axisNames = new ArrayList<>();
        }
        @Override public String name() { return "nest_tensor"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("Nest values into a tensor"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    ArgSpec.any("value", 0, List.of()),
                    ArgSpec.any("axes", 1, List.of()));
        }
        @Override public Schema outputSchema() {
            return new Schema(List.of(new Field("result",
                    new FieldType(true, new ArrowType.Null(), null, Map.of("vgi_type", "any")), null)));
        }
        @Override public Schema bindOutputSchema(Schema inputSchema) {
            if (inputSchema == null || inputSchema.getFields().size() < 2) return outputSchema();
            Field valField = inputSchema.getFields().get(0);
            Field axesField = inputSchema.getFields().get(1);
            int dims = axesField.getChildren().size();
            // tensor type: nested list of valField's type, dims deep.
            ArrowType inner = valField.getType();
            List<Field> nested = null;
            for (int d = 0; d < dims; d++) {
                Field child = new Field("item", new FieldType(true, inner, null), nested);
                nested = List.of(child);
                inner = new ArrowType.List();
            }
            Field tensorField = new Field("tensor",
                    new FieldType(true, inner, null), nested);
            // axes child: struct of (axis_name: list<axis_type>)
            List<Field> axesChildren = new ArrayList<>();
            for (Field f : axesField.getChildren()) {
                axesChildren.add(new Field(f.getName(),
                        new FieldType(true, new ArrowType.List(), null),
                        List.of(new Field("item", new FieldType(true, f.getType(), null), null))));
            }
            Field axesOutField = new Field("axes",
                    new FieldType(true, new ArrowType.Struct(), null), axesChildren);
            Field resultField = new Field("result",
                    new FieldType(true, new ArrowType.Struct(), null),
                    List.of(tensorField, axesOutField));
            return new Schema(List.of(resultField));
        }
        @Override public State newState() { return new State(); }
        @Override public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
            FieldVector vv = input.getVector(0);
            FieldVector av = input.getVector(1);
            int rows = input.getRowCount();
            org.apache.arrow.vector.complex.StructVector axes = av instanceof org.apache.arrow.vector.complex.StructVector
                    ? (org.apache.arrow.vector.complex.StructVector) av : null;
            for (int i = 0; i < rows; i++) {
                State s = states.computeIfAbsent(groupIds[i], k -> new State());
                if (vv.isNull(i)) continue;
                s.values.add(farm.query.vgi.types.ScalarHelpers.toLong(vv, i));
                if (axes != null) {
                    if (s.axisCount < 0) {
                        s.axisCount = axes.getField().getChildren().size();
                        for (Field cf : axes.getField().getChildren()) s.axisNames.add(cf.getName());
                    }
                    long[] coords = new long[s.axisCount];
                    for (int a = 0; a < s.axisCount; a++) {
                        FieldVector cv = axes.getChild(s.axisNames.get(a));
                        coords[a] = cv == null || cv.isNull(i) ? 0
                                : farm.query.vgi.types.ScalarHelpers.toLong(cv, i);
                    }
                    s.axes.add(coords);
                }
            }
        }
        @Override public void combine(State target, State source) {
            target.values.addAll(source.values);
            target.axes.addAll(source.axes);
            if (target.axisCount < 0) {
                target.axisCount = source.axisCount;
                target.axisNames = source.axisNames;
            }
        }
        @Override public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
            org.apache.arrow.vector.FieldVector result = output.getVector("result");
            if (!(result instanceof org.apache.arrow.vector.complex.StructVector outerSv)) {
                result.setNull(rowIndex);
                return;
            }
            // Sort by axes lex order so the tensor + axes are deterministic
            // regardless of input order.
            Integer[] order = new Integer[state.values.size()];
            for (int i = 0; i < order.length; i++) order[i] = i;
            java.util.Arrays.sort(order, (a, b) -> {
                if (state.axes.isEmpty()) return Integer.compare(a, b);
                long[] ax = state.axes.get(a);
                long[] bx = state.axes.get(b);
                for (int d = 0; d < ax.length; d++) {
                    int cmp = Long.compare(ax[d], bx[d]);
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
            org.apache.arrow.vector.complex.StructVector sv =
                    (org.apache.arrow.vector.complex.StructVector) result;
            org.apache.arrow.vector.complex.impl.NullableStructWriter w = sv.getWriter();
            w.setPosition(rowIndex);
            w.start();
            org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter tensorW = w.list("tensor");
            tensorW.startList();
            if (state.axisCount <= 1) {
                for (Integer idx : order) {
                    tensorW.bigInt().writeBigInt(state.values.get(idx));
                }
            } else {
                org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter inner = tensorW.list();
                long currentX = order.length == 0 ? 0 : state.axes.get(order[0])[0];
                inner.startList();
                for (Integer idx : order) {
                    long ax = state.axes.get(idx)[0];
                    if (ax != currentX) {
                        inner.endList();
                        currentX = ax;
                        inner.startList();
                    }
                    inner.bigInt().writeBigInt(state.values.get(idx));
                }
                inner.endList();
            }
            tensorW.endList();
            org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter axesW = w.struct("axes");
            axesW.start();
            for (int axIdx = 0; axIdx < state.axisCount; axIdx++) {
                java.util.TreeSet<Long> uniq = new java.util.TreeSet<>();
                for (long[] coord : state.axes) uniq.add(coord[axIdx]);
                org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter lw =
                        axesW.list(state.axisNames.get(axIdx));
                lw.startList();
                for (Long v : uniq) lw.bigInt().writeBigInt(v);
                lw.endList();
            }
            axesW.end();
            w.end();
            sv.setIndexDefined(rowIndex);
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
