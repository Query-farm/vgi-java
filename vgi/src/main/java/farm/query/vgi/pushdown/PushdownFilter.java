// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import java.util.List;

/**
 * Sealed AST for VGI filter-pushdown predicates. Mirrors vgi-go's
 * {@code Filter} hierarchy. The wire format is a JSON array of filter specs
 * encoded as a single string column inside an Arrow batch; sibling columns
 * carry the actual constant values referenced by {@code value_ref} indices.
 *
 * <p>Currently the AST is read-only — fixtures inspect filters and DuckDB
 * applies them post-emit (auto-apply). A future pass will add per-filter
 * {@code Evaluate} for fixtures that opt out of auto-apply.
 */
public sealed interface PushdownFilter permits
        PushdownFilter.Constant,
        PushdownFilter.IsNull,
        PushdownFilter.IsNotNull,
        PushdownFilter.In,
        PushdownFilter.And,
        PushdownFilter.Or,
        PushdownFilter.Struct {

    /**
     * The name of the column this filter targets.
     *
     * @return the column name from the wire {@code column_name} field
     */
    String columnName();

    /**
     * {@code column_name OP value} — a comparison against a constant value.
     *
     * @param columnName  target column name (wire {@code column_name})
     * @param columnIndex column index into DuckDB's projected schema (wire {@code column_index})
     * @param op          the comparison operator
     * @param value       the constant operand, resolved from the referenced sibling value column
     */
    record Constant(String columnName, int columnIndex, ComparisonOperator op, Object value)
            implements PushdownFilter {}

    /**
     * {@code column_name IS NULL}.
     *
     * @param columnName  target column name (wire {@code column_name})
     * @param columnIndex column index into DuckDB's projected schema (wire {@code column_index})
     */
    record IsNull(String columnName, int columnIndex) implements PushdownFilter {}

    /**
     * {@code column_name IS NOT NULL}.
     *
     * @param columnName  target column name (wire {@code column_name})
     * @param columnIndex column index into DuckDB's projected schema (wire {@code column_index})
     */
    record IsNotNull(String columnName, int columnIndex) implements PushdownFilter {}

    /**
     * {@code column_name IN (values...)} — {@code values} is a list of scalars.
     *
     * @param columnName  target column name (wire {@code column_name})
     * @param columnIndex column index into DuckDB's projected schema (wire {@code column_index})
     * @param values      the membership set, resolved from the referenced sibling value column
     */
    record In(String columnName, int columnIndex, List<Object> values)
            implements PushdownFilter {}

    /**
     * Conjunction of N children.
     *
     * @param columnName  target column name (wire {@code column_name})
     * @param columnIndex column index into DuckDB's projected schema (wire {@code column_index})
     * @param children    the conjoined sub-filters
     */
    record And(String columnName, int columnIndex, List<PushdownFilter> children)
            implements PushdownFilter {}

    /**
     * Disjunction of N children.
     *
     * @param columnName  target column name (wire {@code column_name})
     * @param columnIndex column index into DuckDB's projected schema (wire {@code column_index})
     * @param children    the disjoined sub-filters
     */
    record Or(String columnName, int columnIndex, List<PushdownFilter> children)
            implements PushdownFilter {}

    /**
     * Filter that recurses into a struct field (e.g. {@code col.field IS NOT NULL}).
     *
     * @param columnName  outer struct column name (wire {@code column_name})
     * @param columnIndex column index into DuckDB's projected schema (wire {@code column_index})
     * @param childIndex  index of the struct child the filter applies to (wire {@code child_index})
     * @param childName   name of the struct child the filter applies to (wire {@code child_name})
     * @param childFilter the filter applied to the child field, or {@code null} when unfiltered
     */
    record Struct(String columnName, int columnIndex, int childIndex, String childName,
                    PushdownFilter childFilter) implements PushdownFilter {}
}
