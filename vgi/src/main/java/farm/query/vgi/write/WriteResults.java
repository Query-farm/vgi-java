// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.write;

import farm.query.vgi.internal.VectorScalarCodec;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Row-id fields and result batches for writable catalog tables (port of vgi-python's
 * {@code write_results.py} / vgi-typescript's {@code write-results.ts}).
 *
 * <p>A write function binds {@link #resultSchema(WriteResultMode, Schema)} for the mode
 * {@link WriteOptions#from} decodes, and emits one {@link #resultBatch} per input batch:
 *
 * <pre>{@code
 * public BindResponse onBind(TableInOutBindParams p) {
 *     WriteResultMode mode = WriteOptions.from(p.arguments()).resultMode();
 *     return BindResponse.forSchema(SchemaUtil.serializeSchema(WriteResults.resultSchema(mode, ITEMS)));
 * }
 * // in the exchange, after applying the batch:
 * out.emit(WriteResults.resultBatch(mode, ITEMS, oldRows, newRows, allocator));
 * }</pre>
 */
public final class WriteResults {

    /** Field-metadata key marking a table's row-id column. */
    public static final String ROW_ID_METADATA_KEY = "is_row_id";

    private WriteResults() {}

    /**
     * DuckDB's conventional row-id field, {@code rowid BIGINT NOT NULL}.
     *
     * @return the field
     * @see #rowIdField(String, ArrowType)
     */
    public static Field rowIdField() {
        return rowIdField("rowid", new ArrowType.Int(64, true));
    }

    /**
     * A row-id field for a writable table's columns and its scan's output.
     *
     * <p>DuckDB reads the row id as a <em>virtual</em> column, which it only requests from a
     * scan that supports projection pushdown. A table with UPDATE or DELETE therefore needs a
     * scan function that (1) emits this field, (2) declares projection pushdown
     * ({@code FunctionMetadata#withPushdown(true, ...)}) and emits exactly
     * {@code TableInitParams#outputSchema()}, and (3) is registered in the table's schema. Row
     * ids must be unique and stable for the life of a statement.
     *
     * @param name the column name
     * @param type its type (normally BIGINT)
     * @return the field, carrying {@value #ROW_ID_METADATA_KEY} metadata
     */
    public static Field rowIdField(String name, ArrowType type) {
        return new Field(name, new FieldType(false, type, null, Map.of(ROW_ID_METADATA_KEY, "")), null);
    }

    /**
     * Whether {@code field} is a row-id column.
     *
     * @param field the field
     * @return true when it carries {@value #ROW_ID_METADATA_KEY} metadata
     */
    public static boolean isRowId(Field field) {
        return field.getMetadata() != null && field.getMetadata().containsKey(ROW_ID_METADATA_KEY);
    }

    /**
     * The table's user columns: {@code columns} without its row-id field. Write results
     * ({@code rows}, and both halves of {@code changes}) use this schema.
     *
     * @param columns the table's columns
     * @return the user columns
     */
    public static Schema userColumns(Schema columns) {
        List<Field> out = new ArrayList<>();
        for (Field f : columns.getFields()) if (!isRowId(f)) out.add(f);
        return new Schema(out);
    }

    /**
     * The exact output schema a write function must bind for {@code mode}.
     *
     * @param mode        the requested mode
     * @param tableSchema the table's user columns ({@link #userColumns})
     * @return {@code count BIGINT NOT NULL}; the table schema; or
     *     {@code old STRUCT(...), new STRUCT(...)}
     */
    public static Schema resultSchema(WriteResultMode mode, Schema tableSchema) {
        return switch (mode) {
            case COUNT -> new Schema(List.of(new Field("count",
                    FieldType.notNullable(new ArrowType.Int(64, true)), null)));
            case ROWS -> tableSchema;
            case CHANGES -> new Schema(List.of(
                    new Field("old", FieldType.nullable(ArrowType.Struct.INSTANCE), tableSchema.getFields()),
                    new Field("new", FieldType.nullable(ArrowType.Struct.INSTANCE), tableSchema.getFields())));
        };
    }

    /**
     * A {@code count} result batch. The caller (or {@code OutputCollector.emit}) owns it.
     *
     * @param count     rows written
     * @param allocator the allocator to build it in
     * @return the batch
     */
    public static VectorSchemaRoot countBatch(long count, BufferAllocator allocator) {
        Schema schema = resultSchema(WriteResultMode.COUNT, new Schema(List.of()));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        VectorScalarCodec.write(root.getVector(0), 0, count);
        root.setRowCount(1);
        return root;
    }

    /**
     * A {@code rows} result batch: the written rows, keyed by column name (missing keys are
     * NULL).
     *
     * @param tableSchema the table's user columns
     * @param rows        the rows
     * @param allocator   the allocator to build it in
     * @return the batch
     */
    public static VectorSchemaRoot rowsBatch(Schema tableSchema, List<? extends Map<String, ?>> rows,
                                             BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(tableSchema, allocator);
        try {
            root.allocateNew();
            for (int i = 0; i < rows.size(); i++) {
                Map<String, ?> row = rows.get(i);
                for (FieldVector v : root.getFieldVectors()) {
                    VectorScalarCodec.write(v, i, row == null ? null : row.get(v.getName()));
                }
            }
            root.setRowCount(rows.size());
            return root;
        } catch (RuntimeException e) {
            root.close();
            throw e;
        }
    }

    /**
     * A {@code changes} result batch, one old / new pair per written row: {@code old} is
     * {@code null} for an insert, {@code new} is {@code null} for a delete.
     *
     * @param tableSchema the table's user columns
     * @param oldRows     the before images
     * @param newRows     the after images (same length)
     * @param allocator   the allocator to build it in
     * @return the batch
     */
    public static VectorSchemaRoot changesBatch(Schema tableSchema, List<? extends Map<String, ?>> oldRows,
                                                List<? extends Map<String, ?>> newRows, BufferAllocator allocator) {
        if (oldRows.size() != newRows.size()) {
            throw new IllegalArgumentException("oldRows and newRows must have equal length");
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(resultSchema(WriteResultMode.CHANGES, tableSchema), allocator);
        try {
            root.allocateNew();
            StructVector oldV = (StructVector) root.getVector("old");
            StructVector newV = (StructVector) root.getVector("new");
            for (int i = 0; i < oldRows.size(); i++) {
                writeImage(oldV, i, oldRows.get(i));
                writeImage(newV, i, newRows.get(i));
            }
            root.setRowCount(oldRows.size());
            return root;
        } catch (RuntimeException e) {
            root.close();
            throw e;
        }
    }

    private static void writeImage(StructVector v, int row, Map<String, ?> image) {
        if (image == null) {
            v.setNull(row);
            return;
        }
        for (Field f : v.getField().getChildren()) {
            VectorScalarCodec.write(v.getChild(f.getName()), row, image.get(f.getName()));
        }
        v.setIndexDefined(row);
    }

    /**
     * The result batch for {@code mode}, from the written rows' before / after images:
     * {@code count} counts them, {@code rows} returns each row as written (the after image,
     * or the before image of a delete), {@code changes} returns both.
     *
     * @param mode        the requested mode
     * @param tableSchema the table's user columns
     * @param oldRows     the before images ({@code null} entries for inserts)
     * @param newRows     the after images ({@code null} entries for deletes), same length
     * @param allocator   the allocator to build it in
     * @return the batch
     */
    public static VectorSchemaRoot resultBatch(WriteResultMode mode, Schema tableSchema,
                                               List<? extends Map<String, ?>> oldRows,
                                               List<? extends Map<String, ?>> newRows,
                                               BufferAllocator allocator) {
        return switch (mode) {
            case COUNT -> countBatch(newRows.size(), allocator);
            case CHANGES -> changesBatch(tableSchema, oldRows, newRows, allocator);
            case ROWS -> {
                List<Map<String, ?>> shown = new ArrayList<>(newRows.size());
                for (int i = 0; i < newRows.size(); i++) {
                    shown.add(newRows.get(i) != null ? newRows.get(i) : oldRows.get(i));
                }
                yield rowsBatch(tableSchema, shown, allocator);
            }
        };
    }
}
