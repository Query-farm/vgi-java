// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tensor;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.VectorScalarCodec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code nest_tensor(value ANY, axes STRUCT) -> STRUCT(tensor, axes)} —
 * collects rows from a group into a dense N-D tensor keyed by axes
 * coordinates. Mirrors {@code vgi-python NestTensorFunction}.
 *
 * <p>Output struct shape:
 * <pre>
 *   {tensor: list&lt;list&lt;...&lt;value_type&gt;&gt;&gt;,  // N levels deep
 *    axes:   struct&lt;axis_name: list&lt;coord_type&gt;, ...&gt;}
 * </pre>
 *
 * <p>Each axis's coords are sorted ascending and deduplicated. Unfilled cells
 * are NULL. Duplicate coordinates (in update or after combine) raise an error.
 * Null axes structs are skipped; null coord fields inside a non-null axes
 * struct raise an error.
 */
public final class NestTensorFunction implements AggregateFunction<NestTensorFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        ArrayList<Object> values = new ArrayList<>();
        ArrayList<Object[]> coords = new ArrayList<>();
        ArrayList<String> axisNames;
    }

    private static final FunctionSpec SPEC = FunctionSpec.builder("nest_tensor")
            .description("Collect rows into a dense N-D tensor plus per-axis coordinates")
            .any("value")
            .any("axes")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public Schema outputSchema() {
        return new Schema(List.of(new Field("result",
                new org.apache.arrow.vector.types.pojo.FieldType(true, new ArrowType.Null(), null,
                        Map.of("vgi_type", "any")), null)));
    }

    @Override public Schema bindOutputSchema(Schema inputSchema) {
        if (inputSchema == null || inputSchema.getFields().size() < 2) {
            throw new IllegalArgumentException("nest_tensor: expected 2 arguments (value, axes struct)");
        }
        Field valField = inputSchema.getFields().get(0);
        Field axesField = inputSchema.getFields().get(1);
        if (!(axesField.getType() instanceof ArrowType.Struct)) {
            throw new IllegalArgumentException("nest_tensor: axes argument must be a struct, got "
                    + axesField.getType());
        }
        if (axesField.getChildren().isEmpty()) {
            throw new IllegalArgumentException("nest_tensor: axes struct must have at least one field");
        }
        for (Field f : axesField.getChildren()) {
            ArrowType t = f.getType();
            if (t instanceof ArrowType.FloatingPoint) {
                throw new IllegalArgumentException("nest_tensor: axis '" + f.getName()
                        + "' has floating-point type " + t
                        + "; floats are not supported as coord types (NaN breaks equality)");
            }
            if (t instanceof ArrowType.Struct || t instanceof ArrowType.List
                    || t instanceof ArrowType.LargeList || t instanceof ArrowType.FixedSizeList
                    || t instanceof ArrowType.Map) {
                throw new IllegalArgumentException("nest_tensor: axis '" + f.getName()
                        + "' has nested type " + t + "; only scalar coord types are supported");
            }
        }
        return new Schema(List.of(TensorCodec.buildOutputField("result", valField, axesField)));
    }

    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        FieldVector vv = input.getVector(0);
        FieldVector av = input.getVector(1);
        if (!(av instanceof StructVector axesVec)) {
            throw new IllegalArgumentException("nest_tensor: axes argument must be a struct array");
        }
        List<String> axisNames = new ArrayList<>();
        for (Field f : axesVec.getField().getChildren()) axisNames.add(f.getName());
        int dims = axisNames.size();
        // Track intra-batch duplicates per group.
        Map<Long, HashSet<List<Object>>> seen = new LinkedHashMap<>();
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            long gid = groupIds[i];
            State s = states.computeIfAbsent(gid, k -> new State());
            if (s.axisNames == null) s.axisNames = new ArrayList<>(axisNames);
            if (axesVec.isNull(i)) continue;
            Object[] coords = new Object[dims];
            for (int a = 0; a < dims; a++) {
                FieldVector child = axesVec.getChild(axisNames.get(a));
                if (child == null || child.isNull(i)) {
                    throw new IllegalArgumentException(
                            "NestTensorError: nest_tensor: null coord value for axis '"
                                    + axisNames.get(a) + "' at row " + i + " (group " + gid + ")");
                }
                coords[a] = VectorScalarCodec.read(child, i);
            }
            HashSet<List<Object>> gseen = seen.computeIfAbsent(gid, k -> new HashSet<>());
            // Seed with previously-accumulated coords on first sight of this gid in this batch.
            if (gseen.isEmpty() && !s.coords.isEmpty()) {
                for (Object[] prior : s.coords) gseen.add(Arrays.asList(prior));
            }
            List<Object> key = Arrays.asList(coords);
            if (gseen.contains(key)) {
                Map<String, Object> coordMap = new LinkedHashMap<>();
                for (int a = 0; a < dims; a++) coordMap.put(axisNames.get(a), coords[a]);
                throw new IllegalArgumentException("NestTensorError: nest_tensor: duplicate coordinate "
                        + coordMap + " in group " + gid);
            }
            gseen.add(key);
            Object value = vv.isNull(i) ? null : VectorScalarCodec.read(vv, i);
            s.values.add(value);
            s.coords.add(coords);
        }
    }

    @Override
    public void combine(State target, State source) {
        target.values.addAll(source.values);
        target.coords.addAll(source.coords);
        if (target.axisNames == null) target.axisNames = source.axisNames;
    }

    @Override
    public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
        FieldVector resultVec = output.getVector("result");
        if (!(resultVec instanceof StructVector outerSv)) {
            resultVec.setNull(rowIndex);
            return;
        }
        Field resultField = outerSv.getField();
        Field tensorField = null, axesOutField = null;
        for (Field f : resultField.getChildren()) {
            if ("tensor".equals(f.getName())) tensorField = f;
            else if ("axes".equals(f.getName())) axesOutField = f;
        }
        if (tensorField == null || axesOutField == null) {
            outerSv.setNull(rowIndex);
            return;
        }
        ArrowType valueType = leafType(tensorField);
        int dims = state.axisNames == null ? 0 : state.axisNames.size();

        // Collect sorted distinct coord per axis.
        List<List<Object>> perAxis = new ArrayList<>(dims);
        for (int a = 0; a < dims; a++) {
            List<Object> col = new ArrayList<>(state.coords.size());
            for (Object[] c : state.coords) col.add(c[a]);
            perAxis.add(TensorCodec.sortedDistinct(col));
        }
        int[] shape = new int[dims];
        for (int a = 0; a < dims; a++) shape[a] = perAxis.get(a).size();
        long total = 1;
        for (int s : shape) total *= s;
        // Sparse map: flatIdx -> value. Cells absent from the map → null.
        Map<Long, Object> cellMap = new java.util.HashMap<>();
        Map<Object, Integer>[] idxLookup = new Map[dims];
        for (int a = 0; a < dims; a++) {
            Map<Object, Integer> m = new java.util.HashMap<>();
            List<Object> col = perAxis.get(a);
            for (int i = 0; i < col.size(); i++) m.put(col.get(i), i);
            idxLookup[a] = m;
        }
        for (int r = 0; r < state.values.size(); r++) {
            Object[] c = state.coords.get(r);
            long flat = 0;
            for (int a = 0; a < dims; a++) {
                flat = flat * shape[a] + idxLookup[a].get(c[a]);
            }
            if (cellMap.containsKey(flat)) {
                Map<String, Object> coordMap = new LinkedHashMap<>();
                for (int a = 0; a < dims; a++) coordMap.put(state.axisNames.get(a), c[a]);
                throw new IllegalArgumentException(
                        "NestTensorError: nest_tensor: duplicate coordinate " + coordMap
                                + " (arrived from parallel partitions)");
            }
            cellMap.put(flat, state.values.get(r));
        }

        NullableStructWriter w = outerSv.getWriter();
        w.setPosition(rowIndex);
        w.start();
        BaseWriter.ListWriter tensorW = w.list("tensor");
        writeTensorRecursive(tensorW, dims, shape, 0, 0, valueType, cellMap);

        BaseWriter.StructWriter axesW = w.struct("axes");
        axesW.start();
        for (int a = 0; a < dims; a++) {
            BaseWriter.ListWriter lw = axesW.list(state.axisNames.get(a));
            Field axisField = findAxisField(axesOutField, state.axisNames.get(a));
            ArrowType coordType = axisField == null ? new ArrowType.Int(64, true)
                    : axisField.getChildren().get(0).getType();
            lw.startList();
            for (Object coord : perAxis.get(a)) TensorCodec.writeScalar(lw, coordType, coord);
            lw.endList();
        }
        axesW.end();
        w.end();
        outerSv.setIndexDefined(rowIndex);
    }

    private void writeTensorRecursive(BaseWriter.ListWriter lw, int dims, int[] shape,
                                         int depth, long baseFlat, ArrowType valueType,
                                         Map<Long, Object> cellMap) {
        lw.startList();
        if (dims == 0) {
            lw.endList();
            return;
        }
        if (depth == dims - 1) {
            for (int i = 0; i < shape[depth]; i++) {
                long flat = baseFlat * shape[depth] + i;
                TensorCodec.writeScalar(lw, valueType, cellMap.get(flat));
            }
        } else {
            // Arrow ListWriter exposes one child element-writer; call .list()
            // once and reuse it for every sub-list in this level.
            BaseWriter.ListWriter inner = lw.list();
            for (int i = 0; i < shape[depth]; i++) {
                writeTensorRecursive(inner, dims, shape, depth + 1,
                        baseFlat * shape[depth] + i, valueType, cellMap);
            }
        }
        lw.endList();
    }

    private static ArrowType leafType(Field tensorField) {
        Field inner = tensorField;
        while (inner.getType() instanceof ArrowType.List
                || inner.getType() instanceof ArrowType.LargeList
                || inner.getType() instanceof ArrowType.FixedSizeList) {
            inner = inner.getChildren().get(0);
        }
        return inner.getType();
    }

    private static Field findAxisField(Field axesOutField, String name) {
        for (Field f : axesOutField.getChildren()) if (f.getName().equals(name)) return f;
        return null;
    }

    @Override
    public void finalizeEmpty(VectorSchemaRoot output, int rowIndex) {
        // Empty group → null result. The tests don't cover this explicitly but
        // it's the conservative default.
        output.getVector("result").setNull(rowIndex);
    }
}
