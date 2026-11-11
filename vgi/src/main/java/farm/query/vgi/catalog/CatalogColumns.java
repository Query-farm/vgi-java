// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import farm.query.vgi.internal.ColumnExpressions;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

/**
 * Builders for table columns that carry canonical vgi.v3 column semantics.
 */
public final class CatalogColumns {

    private CatalogColumns() {}

    /**
     * A VIRTUAL generated column: DuckDB computes it from the row's other columns and the
     * worker never emits it. The column is nullable, and the expression is checked against
     * the grammar the extension reads (unqualified columns; NULL, Boolean, integer and string
     * literals; casts to BOOLEAN/TINYINT/SMALLINT/INTEGER/BIGINT/VARCHAR; comparisons,
     * AND/OR/NOT, IS [NOT] NULL, COALESCE, {@code + - * % ||}, abs, lower, upper).
     *
     * <p>Replaces the pre-vgi.v3 {@code generated_expression} column metadata, which the
     * framework still translates to this form when it serves a table.
     *
     * @param name       the column name
     * @param type       BOOLEAN, a signed 8-64 bit integer, or UTF-8
     * @param sql        the DuckDB scalar expression, e.g. {@code "n * 2"}
     * @param schemaPath the owning table's schema path
     * @return the column
     * @throws IllegalArgumentException if the type or expression is one DuckDB cannot read
     */
    public static Field virtualGenerated(String name, ArrowType type, String sql, List<String> schemaPath) {
        Field field = new Field(name, new FieldType(true, type, null,
                ColumnExpressions.virtualMetadata(sql, schemaPath)), null);
        ColumnExpressions.validate(new org.apache.arrow.vector.types.pojo.Schema(List.of(field)), schemaPath, true);
        return field;
    }

    /**
     * {@link #virtualGenerated(String, ArrowType, String, List)} for a one-component schema.
     *
     * @param name   the column name
     * @param type   BOOLEAN, a signed 8-64 bit integer, or UTF-8
     * @param sql    the DuckDB scalar expression
     * @param schema the owning table's schema
     * @return the column
     */
    public static Field virtualGenerated(String name, ArrowType type, String sql, String schema) {
        return virtualGenerated(name, type, sql, List.of(schema));
    }

    /**
     * {@code field} with a column default: DuckDB fills it into an {@code INSERT} that omits the
     * column (and into {@code DEFAULT VALUES}). The expression must be constant, e.g.
     * {@code "0"}, {@code "'n/a'"} or {@code "CAST(0 AS BIGINT)"}; the extension binds it once,
     * against the column's type. The column keeps its name, type, nullability, children and other
     * metadata.
     *
     * @param field      the column
     * @param sql        the DuckDB constant expression
     * @param schemaPath the owning table's schema path
     * @return the column, carrying canonical {@code vgi.column.default.v1} metadata
     * @throws IllegalArgumentException if the expression is empty or the column is generated
     */
    public static Field withDefault(Field field, String sql, List<String> schemaPath) {
        java.util.Map<String, String> metadata = new java.util.LinkedHashMap<>(field.getMetadata());
        if (metadata.containsKey(ColumnExpressions.GENERATED)
                || metadata.containsKey(ColumnExpressions.LEGACY_GENERATED)) {
            throw new IllegalArgumentException("Column " + field.getName() + ": a generated column cannot have a default");
        }
        metadata.remove("default");
        metadata.putAll(ColumnExpressions.defaultMetadata(sql, schemaPath));
        return new Field(field.getName(), new FieldType(field.isNullable(), field.getType(), field.getDictionary(),
                metadata), field.getChildren());
    }

    /**
     * {@link #withDefault(Field, String, List)} for a one-component schema.
     *
     * @param field  the column
     * @param sql    the DuckDB constant expression
     * @param schema the owning table's schema
     * @return the column, carrying canonical {@code vgi.column.default.v1} metadata
     */
    public static Field withDefault(Field field, String sql, String schema) {
        return withDefault(field, sql, List.of(schema));
    }
}
