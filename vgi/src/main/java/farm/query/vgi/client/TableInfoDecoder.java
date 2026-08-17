// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.protocol.TableInfo;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decode the {@link TableInfo} records a worker returns from
 * {@code catalog_schema_contents_tables} and {@code catalog_table_get}.
 *
 * <p>The inverse of the worker-side {@code TableInfoSerializer}. It exists
 * separately because {@code TableInfo} is the one catalog record that is
 * <em>not</em> an {@code ArrowSerializableRecord} — its
 * {@code list<list<int32>>} constraint shapes and its several optional binary
 * "inline" fields are hand-rolled on the way out — so
 * {@code RecordCodec.deserializeFromBytes} cannot read one back. Without this
 * a JVM consumer can list a catalog's <em>functions</em> but not its
 * <em>tables</em>, which is precisely the half a Spark {@code TableCatalog}
 * needs.
 *
 * <pre>{@code
 * ItemsResponse tables = vgi.catalog_schema_contents_tables(handle, "data", null, null);
 * for (TableInfo t : TableInfoDecoder.decodeAll(tables.items())) {
 *     Schema columns = SchemaUtil.deserializeSchema(t.columns());
 * }
 * }</pre>
 *
 * <h2>Decoded against the wire's own schema</h2>
 *
 * <p>Fields are matched to record components <strong>by name</strong>, read
 * through the schema the sender actually put on the batch rather than through
 * a copy of the schema this repo happens to write. That is deliberate: the
 * decoder is aimed at <em>other</em> implementations' bytes (vgi-python is the
 * reference), and a positional read would silently transpose two same-typed
 * columns if a sender ever ordered them differently. A field the sender omits
 * decodes to its empty/absent value — additive wire growth is the normal case
 * here ({@code required_filters} is a trailing addition) — while the four
 * fields a table cannot be identified without are required outright.
 */
public final class TableInfoDecoder {

    private TableInfoDecoder() {}

    /**
     * Decode one serialised {@code TableInfo} item.
     *
     * @param item the IPC bytes of a single item from an {@code ItemsResponse}
     * @return the decoded table metadata
     * @throws IllegalStateException if the bytes are missing, empty, or not a
     *         single-row {@code TableInfo} batch
     */
    public static TableInfo decode(byte[] item) {
        if (item == null || item.length == 0) {
            throw new IllegalStateException("TableInfoDecoder.decode: empty item bytes");
        }
        Map<String, Object> row;
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(item), Allocators.root())) {
            if (r.readNextBatch() == null) {
                throw new IllegalStateException("TableInfo item carried no batch");
            }
            VectorSchemaRoot root = r.root();
            if (root.getRowCount() != 1) {
                throw new IllegalStateException(
                        "TableInfo item must be a single row, got " + root.getRowCount());
            }
            row = Marshalling.decodeRow(root, r.dictionaryProvider(), r.wireSchema());
        } catch (Exception e) {
            if (e instanceof IllegalStateException ise) throw ise;
            throw new IllegalStateException("TableInfoDecoder.decode failed", e);
        }
        return fromRow(row);
    }

    /**
     * Decode every item of an {@code ItemsResponse}.
     *
     * @param items the response's items, or {@code null}
     * @return the decoded tables, in the worker's order; empty when there are none
     */
    public static List<TableInfo> decodeAll(List<byte[]> items) {
        if (items == null || items.isEmpty()) return List.of();
        List<TableInfo> out = new ArrayList<>(items.size());
        for (byte[] item : items) out.add(decode(item));
        return Collections.unmodifiableList(out);
    }

    // ------------------------------------------------------------------

    private static TableInfo fromRow(Map<String, Object> row) {
        return new TableInfo(
                str(row, "comment"),
                stringMap(row, "tags"),
                required(str(row, "name"), "name"),
                required(str(row, "schema_name"), "schema_name"),
                required(bytes(row, "columns"), "columns"),
                intList(row, "not_null_constraints"),
                intListList(row, "unique_constraints"),
                stringList(row, "check_constraints"),
                intListList(row, "primary_key_constraints"),
                bytesList(row, "foreign_key_constraints"),
                bool(row, "supports_insert"),
                bool(row, "supports_update"),
                bool(row, "supports_delete"),
                bool(row, "supports_returning"),
                bool(row, "supports_column_statistics"),
                bytes(row, "scan_function"),
                bytes(row, "insert_function"),
                bytes(row, "update_function"),
                bytes(row, "delete_function"),
                nullableLong(row, "cardinality_estimate"),
                nullableLong(row, "cardinality_max"),
                bytes(row, "column_statistics"),
                bytes(row, "bind_result"),
                stringListList(row, "required_filters"));
    }

    private static <T> T required(T value, String field) {
        if (value == null) {
            throw new IllegalStateException("TableInfo is missing required field '" + field + "'");
        }
        return value;
    }

    private static String str(Map<String, Object> row, String field) {
        Object v = row.get(field);
        return v == null ? null : v.toString();
    }

    private static byte[] bytes(Map<String, Object> row, String field) {
        return (byte[]) row.get(field);
    }

    private static boolean bool(Map<String, Object> row, String field) {
        return Boolean.TRUE.equals(row.get(field));
    }

    /**
     * {@code cardinality_estimate} / {@code cardinality_max} ride a schema that
     * declares them non-nullable while the row's null bit may still be set —
     * see {@code TableInfoSerializer}'s note on why. So "absent" here is a null
     * value on a non-nullable field, not a missing column.
     */
    private static Long nullableLong(Map<String, Object> row, String field) {
        Object v = row.get(field);
        return v instanceof Number n ? n.longValue() : null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Map<String, Object> row, String field) {
        Object v = row.get(field);
        if (!(v instanceof Map<?, ?> m)) return Map.of();
        Map<String, String> out = new LinkedHashMap<>(m.size());
        for (Map.Entry<?, ?> e : ((Map<Object, Object>) m).entrySet()) {
            if (e.getKey() != null) {
                out.put(e.getKey().toString(), e.getValue() == null ? null : e.getValue().toString());
            }
        }
        return Collections.unmodifiableMap(out);
    }

    private static List<Object> rawList(Map<String, Object> row, String field) {
        Object v = row.get(field);
        // Not List.copyOf: an Arrow list may legitimately carry null elements.
        return v instanceof List<?> l ? new ArrayList<>(l) : List.of();
    }

    private static List<String> stringList(Map<String, Object> row, String field) {
        List<Object> raw = rawList(row, field);
        List<String> out = new ArrayList<>(raw.size());
        for (Object o : raw) out.add(o == null ? null : o.toString());
        return Collections.unmodifiableList(out);
    }

    private static List<byte[]> bytesList(Map<String, Object> row, String field) {
        List<Object> raw = rawList(row, field);
        List<byte[]> out = new ArrayList<>(raw.size());
        for (Object o : raw) out.add((byte[]) o);
        return Collections.unmodifiableList(out);
    }

    /** Arrow int32 arrives as {@code Long} (see {@code Marshalling.readScalar}). */
    private static List<Integer> intList(Map<String, Object> row, String field) {
        List<Object> raw = rawList(row, field);
        List<Integer> out = new ArrayList<>(raw.size());
        for (Object o : raw) out.add(o == null ? null : ((Number) o).intValue());
        return Collections.unmodifiableList(out);
    }

    private static List<List<Integer>> intListList(Map<String, Object> row, String field) {
        List<Object> raw = rawList(row, field);
        List<List<Integer>> out = new ArrayList<>(raw.size());
        for (Object group : raw) {
            List<Integer> inner = new ArrayList<>();
            if (group instanceof List<?> l) {
                for (Object o : l) inner.add(o == null ? null : ((Number) o).intValue());
            }
            out.add(Collections.unmodifiableList(inner));
        }
        return Collections.unmodifiableList(out);
    }

    private static List<List<String>> stringListList(Map<String, Object> row, String field) {
        List<Object> raw = rawList(row, field);
        List<List<String>> out = new ArrayList<>(raw.size());
        for (Object group : raw) {
            List<String> inner = new ArrayList<>();
            if (group instanceof List<?> l) {
                for (Object o : l) inner.add(o == null ? null : o.toString());
            }
            out.add(Collections.unmodifiableList(inner));
        }
        return Collections.unmodifiableList(out);
    }
}
