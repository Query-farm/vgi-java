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
}
