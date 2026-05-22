// Copyright 2025-2026 Query.Farm LLC

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

    String columnName();

    /** {@code column_name OP value} — six comparison operators. */
    record Constant(String columnName, int columnIndex, ComparisonOperator op, Object value)
            implements PushdownFilter {}

    record IsNull(String columnName, int columnIndex) implements PushdownFilter {}

    record IsNotNull(String columnName, int columnIndex) implements PushdownFilter {}

    /** {@code column_name IN (values...)} — values is a list of scalars. */
    record In(String columnName, int columnIndex, List<Object> values)
            implements PushdownFilter {}

    /** Conjunction of N children. */
    record And(String columnName, int columnIndex, List<PushdownFilter> children)
            implements PushdownFilter {}

    /** Disjunction of N children. */
    record Or(String columnName, int columnIndex, List<PushdownFilter> children)
            implements PushdownFilter {}

    /** Filter that recurses into a struct field (e.g. {@code col.field IS NOT NULL}). */
    record Struct(String columnName, int columnIndex, int childIndex, String childName,
                    PushdownFilter childFilter) implements PushdownFilter {}
}
