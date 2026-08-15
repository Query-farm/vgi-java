// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.internal.VectorScalarCodec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A single wire constant: a Java value paired with the Arrow type it is
 * encoded as.
 *
 * <p>Every client-side encoder in this package — bind arguments, settings,
 * pushdown-filter constants, join keys — needs the same two things for each
 * value it writes: an Arrow {@link Field} to declare and a cell to populate.
 * The type cannot always be inferred from the value, because {@code null} is a
 * legitimate constant with no runtime class to inspect and because Java's
 * {@code Long} covers every DuckDB integer width. So the type travels with the
 * value rather than being re-derived at each write site.
 *
 * <p>{@link #of(Object)} infers the type for the common cases and is what the
 * convenience overloads throughout this package call. Reach for
 * {@link #of(ArrowType, Object)} when the width matters — notably for a
 * pushdown-filter constant, where the worker compares the literal against a
 * column of a specific type — and for {@link #ofNull(ArrowType)} whenever the
 * value is {@code null}.
 *
 * @param type  the Arrow type the value is written as; never {@code null}
 * @param value the Java value, or {@code null} for a null cell
 */
public record ScalarValue(ArrowType type, Object value) {

    /** Signed 64-bit integer — the default inferred for every boxed Java integer. */
    public static final ArrowType INT64 = new ArrowType.Int(64, true);
    /** Signed 32-bit integer. */
    public static final ArrowType INT32 = new ArrowType.Int(32, true);
    /** Double-precision float — the default inferred for {@code Float} and {@code Double}. */
    public static final ArrowType FLOAT64 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    /** Variable-length UTF-8 string. */
    public static final ArrowType UTF8 = new ArrowType.Utf8();
    /** Boolean. */
    public static final ArrowType BOOL = new ArrowType.Bool();
    /** Variable-length byte string. */
    public static final ArrowType BINARY = new ArrowType.Binary();

    /**
     * Validates that a type is present — a {@code ScalarValue} without one
     * could not declare its field.
     */
    public ScalarValue {
        if (type == null) throw new IllegalArgumentException("ScalarValue.type must not be null");
    }

    /**
     * A value whose Arrow type is inferred from its Java class.
     *
     * <p>Boxed integers become {@code int64}, {@code Float}/{@code Double}
     * become {@code float64}, {@code String} becomes {@code utf8},
     * {@code Boolean} becomes {@code bool}, {@code byte[]} becomes
     * {@code binary}. A {@link Map} becomes a struct and a {@link List} a list,
     * with child types inferred the same way (both must be non-empty, and a
     * list's elements must share one type).
     *
     * <p>An argument that is already a {@code ScalarValue} passes through
     * unchanged, so every {@code Object}-taking convenience in this package
     * also accepts an explicitly typed value.
     *
     * @param value the value to wrap; must not be {@code null}
     * @return the typed value
     * @throws IllegalArgumentException if {@code value} is {@code null} (use
     *         {@link #ofNull(ArrowType)}) or its class has no inferable Arrow type
     */
    public static ScalarValue of(Object value) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "cannot infer an Arrow type for null — use ScalarValue.ofNull(type)");
        }
        if (value instanceof ScalarValue already) return already;
        return new ScalarValue(inferType(value), value);
    }

    /**
     * A value written as an explicitly chosen Arrow type — e.g. an
     * {@code int32} filter constant against an {@code INTEGER} column.
     *
     * @param type  the Arrow type to encode as
     * @param value the value, or {@code null} for a null cell
     * @return the typed value
     */
    public static ScalarValue of(ArrowType type, Object value) {
        return new ScalarValue(type, value);
    }

    /**
     * A typed null. The type is still required: the field has to be declared
     * before the null bit can be set.
     *
     * @param type the Arrow type of the (null) cell
     * @return the typed null value
     */
    public static ScalarValue ofNull(ArrowType type) {
        return new ScalarValue(type, null);
    }

    /**
     * Declare this value as a nullable field.
     *
     * @param name the field name
     * @return a nullable {@link Field} of this value's type
     */
    public Field field(String name) {
        return new Field(name, new FieldType(true, type, null), childFields());
    }

    /**
     * Write this value into row {@code row} of {@code vector}.
     *
     * @param vector the destination vector, allocated from {@link #field(String)}
     * @param row    the row index to write
     */
    public void write(FieldVector vector, int row) {
        VectorScalarCodec.write(vector, row, value);
    }

    /** Nested field declarations for the struct / list cases; {@code null} for scalars. */
    private List<Field> childFields() {
        if (value instanceof Map<?, ?> map) {
            List<Field> children = new ArrayList<>(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                children.add(of(e.getValue()).field(String.valueOf(e.getKey())));
            }
            return children;
        }
        if (value instanceof List<?> list) {
            return List.of(of(list.get(0)).field("item"));
        }
        return null;
    }

    private static ArrowType inferType(Object value) {
        if (value instanceof Boolean) return BOOL;
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return INT64;
        }
        if (value instanceof Float || value instanceof Double) return FLOAT64;
        if (value instanceof String) return UTF8;
        if (value instanceof byte[]) return BINARY;
        if (value instanceof Map<?, ?> map) {
            if (map.isEmpty()) {
                throw new IllegalArgumentException(
                        "cannot infer a struct type from an empty map — use ScalarValue.of(type, value)");
            }
            // Child fields (and their order) come from childFields(), which
            // follows the map's iteration order — pass a LinkedHashMap when the
            // struct's field order matters to the worker.
            return new ArrowType.Struct();
        }
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                throw new IllegalArgumentException(
                        "cannot infer a list type from an empty list — use ScalarValue.of(type, value)");
            }
            return new ArrowType.List();
        }
        throw new IllegalArgumentException(
                "no Arrow type inference for " + value.getClass().getName()
                        + " — use ScalarValue.of(type, value)");
    }
}
