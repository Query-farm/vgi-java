// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.types;

import farm.query.vgi.internal.SchemaUtil;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Convenience builders for {@link Schema}s and their IPC byte encodings —
 * primarily used by scalar functions that return a single fixed-type column.
 */
public final class Schemas {

    /** Signed 64-bit integer (SQL {@code BIGINT}). */
    public static final ArrowType INT64 = new ArrowType.Int(64, true);
    /** Signed 32-bit integer (SQL {@code INTEGER}). */
    public static final ArrowType INT32 = new ArrowType.Int(32, true);
    /** Unsigned 32-bit integer (SQL {@code UINTEGER}). */
    public static final ArrowType UINT32 = new ArrowType.Int(32, false);
    /** Unsigned 64-bit integer (SQL {@code UBIGINT}). */
    public static final ArrowType UINT64 = new ArrowType.Int(64, false);
    /** Double-precision float (SQL {@code DOUBLE}). */
    public static final ArrowType FLOAT64 = new ArrowType.FloatingPoint(
            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
    /** Variable-length UTF-8 string (SQL {@code VARCHAR}). */
    public static final ArrowType UTF8 = new ArrowType.Utf8();
    /** Boolean (SQL {@code BOOLEAN}). */
    public static final ArrowType BOOL = new ArrowType.Bool();
    /** Variable-length byte string (SQL {@code BLOB}). */
    public static final ArrowType BINARY = new ArrowType.Binary();

    private Schemas() {}

    /**
     * Nullable {@link Field} builder — the default for fixture output columns.
     *
     * @param name field name
     * @param type Arrow type
     * @return a nullable field
     */
    public static Field nullable(String name, ArrowType type) {
        return new Field(name, new FieldType(true, type, null), null);
    }

    /**
     * Non-null {@link Field} builder.
     *
     * @param name field name
     * @param type Arrow type
     * @return a non-nullable field
     */
    public static Field nonNull(String name, ArrowType type) {
        return new Field(name, new FieldType(false, type, null), null);
    }

    /**
     * Build a {@link Schema} from a varargs list of fields.
     *
     * @param fields the schema's fields, in order
     * @return the assembled schema
     */
    public static Schema of(Field... fields) {
        return new Schema(List.of(fields));
    }

    /**
     * Microsecond-precision timestamp type, optionally with a timezone
     * ({@code null} or empty for naive timestamps).
     *
     * @param tz IANA timezone name, or {@code null}/empty for a naive timestamp
     * @return the timestamp Arrow type
     */
    public static ArrowType timestampMicros(String tz) {
        return new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.MICROSECOND,
                tz == null || tz.isEmpty() ? null : tz);
    }

    /**
     * {@code list<item>} {@link Field} with a child field literally named
     * {@code "item"} (Arrow convention). {@code nullable} applies to the list
     * itself; the item is always nullable.
     *
     * @param name list field name
     * @param item element Arrow type
     * @param nullable whether the list field itself is nullable
     * @return the list field
     */
    public static Field list(String name, ArrowType item, boolean nullable) {
        Field child = new Field("item", new FieldType(true, item, null), null);
        return new Field(name, new FieldType(nullable, new ArrowType.List(), null), List.of(child));
    }

    /**
     * Single-column nullable {@code result} schema.
     *
     * @param t the result column's Arrow type
     * @return a schema with one nullable field named {@code result}
     */
    public static Schema singleResult(ArrowType t) {
        return new Schema(List.of(new Field("result", new FieldType(true, t, null), null)));
    }

    /**
     * IPC byte encoding of {@link #singleResult(ArrowType)}.
     *
     * @param t the result column's Arrow type
     * @return the IPC-encoded single-column schema
     */
    public static byte[] singleResultIpc(ArrowType t) {
        return SchemaUtil.serializeSchema(singleResult(t));
    }

    /**
     * Single-column "ANY"-typed result schema. The field carries the
     * {@code vgi_type=any} metadata that DuckDB's catalog-enumeration path
     * recognises and reports as {@code ANY} in {@code duckdb_functions()}.
     *
     * @return the IPC-encoded ANY-typed single-column schema
     */
    public static byte[] singleResultAnyIpc() {
        return SchemaUtil.serializeSchema(new Schema(List.of(
                new Field("result",
                        new FieldType(true, new org.apache.arrow.vector.types.pojo.ArrowType.Null(),
                                null, java.util.Map.of("vgi_type", "any")),
                        null))));
    }
}
