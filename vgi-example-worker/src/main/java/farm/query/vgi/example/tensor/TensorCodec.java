// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.tensor;

import farm.query.vgi.internal.VectorScalarCodec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared utilities for nest_tensor / unnest_tensor / unnest_tensor_rows.
 *
 * <p>A "tensor" in this fixture is a struct {@code {tensor, axes}} where
 * {@code tensor} is a {@code list<list<...<value>>>} nested N deep (N = #axes)
 * and {@code axes} is a struct of {@code list<axis_type>}, each axis listing
 * its distinct coordinate values in ascending order.
 */
public final class TensorCodec {

    private TensorCodec() {}

    /**
     * Build the output struct type for nest_tensor given the value type and
     * axes struct type. Mirrors Python's {@code _output_struct_type}.
     */
    public static Field buildOutputField(String name, Field valueField, Field axesField) {
        int dims = axesField.getChildren().size();
        // Build nested list type: list<list<...<value>>>, dims deep.
        Field current = new Field("item",
                new FieldType(true, valueField.getType(), null), valueField.getChildren());
        for (int d = 0; d < dims; d++) {
            current = new Field(d == dims - 1 ? "tensor" : "item",
                    new FieldType(true, new ArrowType.List(), null),
                    List.of(current));
        }
        Field tensorField = current;
        List<Field> axesChildren = new ArrayList<>(dims);
        for (Field f : axesField.getChildren()) {
            axesChildren.add(new Field(f.getName(),
                    new FieldType(true, new ArrowType.List(), null),
                    List.of(new Field("item", new FieldType(true, f.getType(), null), null))));
        }
        Field axesOut = new Field("axes",
                new FieldType(true, new ArrowType.Struct(), null), axesChildren);
        return new Field(name, new FieldType(true, new ArrowType.Struct(), null),
                List.of(tensorField, axesOut));
    }

    /**
     * Bind output for {@code unnest_tensor} scalar: returns
     * {@code list<struct<value, axes>>} where {@code axes} is a flat struct
     * of axis-name → coordinate values.
     */
    public static Schema unnestScalarOutput(Field tensorInputField) {
        ValidatedTensor v = validateTensorInput("unnest_tensor", tensorInputField);
        Field row = unnestRowField(v);
        Field result = new Field("result",
                new FieldType(true, new ArrowType.List(), null), List.of(row));
        return new Schema(List.of(result));
    }

    /** Bind output for {@code unnest_tensor_rows} TIO: flat {value, axes}. */
    public static Schema unnestRowsOutput(Field tensorInputField) {
        ValidatedTensor v = validateTensorInput("unnest_tensor_rows", tensorInputField);
        Field valueOut = new Field("value",
                new FieldType(true, v.valueType.getType(), null), v.valueType.getChildren());
        List<Field> axesChildren = new ArrayList<>(v.axesType.size());
        for (Field f : v.axesType) {
            // Strip the outer list to get the scalar coord type.
            Field inner = f.getChildren().get(0);
            axesChildren.add(new Field(f.getName(),
                    new FieldType(true, inner.getType(), null), inner.getChildren()));
        }
        Field axesOut = new Field("axes",
                new FieldType(true, new ArrowType.Struct(), null), axesChildren);
        return new Schema(List.of(valueOut, axesOut));
    }

    /** Cached shape of a validated tensor input. */
    public static final class ValidatedTensor {
        public final Field valueType;
        public final List<Field> axesType;
        public ValidatedTensor(Field v, List<Field> a) { this.valueType = v; this.axesType = a; }
    }

    private static Field unnestRowField(ValidatedTensor v) {
        Field valueOut = new Field("value",
                new FieldType(true, v.valueType.getType(), null), v.valueType.getChildren());
        List<Field> axesChildren = new ArrayList<>(v.axesType.size());
        for (Field f : v.axesType) {
            Field inner = f.getChildren().get(0);
            axesChildren.add(new Field(f.getName(),
                    new FieldType(true, inner.getType(), null), inner.getChildren()));
        }
        Field axesOut = new Field("axes",
                new FieldType(true, new ArrowType.Struct(), null), axesChildren);
        return new Field("item", new FieldType(true, new ArrowType.Struct(), null),
                List.of(valueOut, axesOut));
    }

    /** Walk the input struct and validate its shape. */
    public static ValidatedTensor validateTensorInput(String fnName, Field inputField) {
        if (!(inputField.getType() instanceof ArrowType.Struct)) {
            throw new IllegalArgumentException(fnName + ": argument must be a struct, got "
                    + inputField.getType());
        }
        Field tensorField = null, axesField = null;
        for (Field f : inputField.getChildren()) {
            if ("tensor".equals(f.getName())) tensorField = f;
            else if ("axes".equals(f.getName())) axesField = f;
        }
        if (tensorField == null || axesField == null) {
            throw new IllegalArgumentException(fnName + ": struct must have 'tensor' and 'axes' fields");
        }
        if (!(axesField.getType() instanceof ArrowType.Struct)) {
            throw new IllegalArgumentException(fnName + ": 'axes' field must be a struct");
        }
        int depth = 0;
        Field inner = tensorField;
        while (inner.getType() instanceof ArrowType.List
                || inner.getType() instanceof ArrowType.LargeList
                || inner.getType() instanceof ArrowType.FixedSizeList) {
            depth++;
            inner = inner.getChildren().get(0);
        }
        if (depth != axesField.getChildren().size()) {
            throw new IllegalArgumentException(fnName + ": tensor nesting depth " + depth
                    + " does not match number of axes " + axesField.getChildren().size());
        }
        return new ValidatedTensor(inner, axesField.getChildren());
    }

    /** Read a tensor struct (one row of a StructVector) into Java structures. */
    public static TensorRow readTensorRow(StructVector sv, int row) {
        if (sv.isNull(row)) return null;
        FieldVector tv = sv.getChild("tensor");
        FieldVector av = sv.getChild("axes");
        Object tensor = VectorScalarCodec.read(tv, row);
        Map<String, List<Object>> axes = new LinkedHashMap<>();
        if (av instanceof StructVector axesSv) {
            for (Field f : axesSv.getField().getChildren()) {
                FieldVector child = axesSv.getChild(f.getName());
                @SuppressWarnings("unchecked")
                List<Object> coords = (List<Object>) VectorScalarCodec.read(child, row);
                axes.put(f.getName(), coords == null ? List.of() : coords);
            }
        }
        return new TensorRow(tensor, axes);
    }

    public static final class TensorRow {
        public final Object tensor;
        public final Map<String, List<Object>> axes;
        public TensorRow(Object tensor, Map<String, List<Object>> axes) {
            this.tensor = tensor;
            this.axes = axes;
        }
    }

    /** Walk a nested tensor by index tuple, returning the leaf value. */
    @SuppressWarnings("unchecked")
    public static Object walkTensor(Object tensor, int[] idx) {
        Object current = tensor;
        for (int d : idx) {
            if (!(current instanceof List<?> list) || d >= list.size()) return null;
            current = list.get(d);
        }
        return current;
    }

    /**
     * Sort {@code values} ascending; coord values are heterogeneous so we use
     * a polymorphic Comparable comparator. Throws on incomparable mixes.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static List<Object> sortedDistinct(List<Object> values) {
        java.util.TreeSet<Object> set = new java.util.TreeSet<>((Comparator<Object>) (a, b) -> {
            if (a == null && b == null) return 0;
            if (a == null) return -1;
            if (b == null) return 1;
            if (a instanceof Comparable ca) return ca.compareTo(b);
            return a.toString().compareTo(b.toString());
        });
        for (Object v : values) set.add(v);
        return new ArrayList<>(set);
    }

    /**
     * Recursively write a nested list value into a ListWriter.
     *
     * @param lw     the outer list writer (positioned via setPosition by caller)
     * @param depth  remaining list depth (1 = innermost)
     * @param valueType  the leaf Arrow type for selecting the right writer
     * @param value  Java value (null, scalar, or List)
     */
    @SuppressWarnings("unchecked")
    public static void writeNestedList(BaseWriter.ListWriter lw, int depth,
                                          ArrowType valueType, Object value) {
        lw.startList();
        if (value instanceof List<?> list) {
            if (depth == 1) {
                for (Object item : list) writeScalar(lw, valueType, item);
            } else {
                for (Object item : list) {
                    if (item instanceof List<?>) {
                        writeNestedList(lw.list(), depth - 1, valueType, item);
                    } else {
                        // Should be a list at this depth; treat malformed as null.
                        BaseWriter.ListWriter inner = lw.list();
                        inner.startList();
                        inner.endList();
                    }
                }
            }
        }
        lw.endList();
    }

    /** Write a single scalar into a ListWriter at the leaf level. */
    public static void writeScalar(BaseWriter.ListWriter lw, ArrowType type, Object value) {
        // Important: a null scalar inside a list is written via the typed
        // sub-writer's writeNull(), not lw.writeNull() — the latter writes a
        // null at the parent (list-element) level, which flattens the list.
        switch (type) {
            case ArrowType.Int it -> {
                if (it.getBitWidth() == 64) {
                    if (value == null) lw.bigInt().writeNull();
                    else lw.bigInt().writeBigInt(((Number) value).longValue());
                } else if (it.getBitWidth() == 32) {
                    if (value == null) lw.integer().writeNull();
                    else lw.integer().writeInt(((Number) value).intValue());
                } else if (it.getBitWidth() == 16) {
                    if (value == null) lw.smallInt().writeNull();
                    else lw.smallInt().writeSmallInt(((Number) value).shortValue());
                } else {
                    if (value == null) lw.tinyInt().writeNull();
                    else lw.tinyInt().writeTinyInt(((Number) value).byteValue());
                }
            }
            case ArrowType.FloatingPoint fp -> {
                if (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE) {
                    if (value == null) lw.float8().writeNull();
                    else lw.float8().writeFloat8(((Number) value).doubleValue());
                } else {
                    if (value == null) lw.float4().writeNull();
                    else lw.float4().writeFloat4(((Number) value).floatValue());
                }
            }
            case ArrowType.Bool b -> {
                if (value == null) lw.bit().writeNull();
                else lw.bit().writeBit(((Boolean) value) ? 1 : 0);
            }
            case ArrowType.Utf8 u -> {
                if (value == null) lw.varChar().writeNull();
                else writeListVarChar(lw, ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            case ArrowType.Date d -> {
                if (value == null) lw.dateDay().writeNull();
                else lw.dateDay().writeDateDay(((Number) value).intValue());
            }
            default -> {
                if (value == null) lw.bigInt().writeNull();
                else if (value instanceof Number n) lw.bigInt().writeBigInt(n.longValue());
                else if (value instanceof String s) writeListVarChar(lw, s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                else throw new IllegalArgumentException("TensorCodec.writeScalar: unsupported type " + type);
            }
        }
    }

    private static void writeListVarChar(BaseWriter.ListWriter lw, byte[] bytes) {
        try (org.apache.arrow.memory.ArrowBuf tmp =
                farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
            tmp.setBytes(0, bytes);
            lw.varChar().writeVarChar(0, bytes.length, tmp);
        }
    }

    /**
     * Write a scalar into a {@link FieldVector} at a given row, dispatching on
     * the destination type. Wraps {@link VectorScalarCodec#write}.
     */
    public static void writeVectorScalar(FieldVector v, int row, Object value) {
        VectorScalarCodec.write(v, row, value);
    }
}
