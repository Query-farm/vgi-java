// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Container for the parsed top-level filter list (implicit AND).
 *
 * @param filters the top-level filters, conjoined as a logical AND
 * @param version the pushdown wire-format version (currently {@code "1"})
 */
public record PushdownFilters(List<PushdownFilter> filters, String version) {

    /**
     * An empty filter set at the current supported version.
     *
     * @return a {@code PushdownFilters} with no filters
     */
    public static PushdownFilters empty() {
        return new PushdownFilters(List.of(), "1");
    }

    /**
     * Format the filters as a human-readable {@code AND}-joined SQL-like
     * string with values inlined. Used by diagnostic fixtures like
     * {@code filter_echo}; matches vgi-go's {@code formatFiltersInline}.
     *
     * @return the inline representation, or {@code "(none)"} when empty
     */
    public String formatInline() {
        if (filters.isEmpty()) return "(none)";
        return filters.stream()
                .map(PushdownFilters::formatOne)
                .collect(Collectors.joining(" AND "));
    }

    /**
     * Python-{@code repr}-style format that wraps each leaf in its filter
     * class name (e.g. {@code ConstantFilter(n < 9500)}, {@code IsNullFilter(c
     * IS NULL)}). The dynamic-filter integration test asserts {@code LIKE
     * '%ConstantFilter(n <%'} on this representation to confirm the C++
     * extension extracted the dynamic bound from inside a
     * {@code ConjunctionAndFilter}.
     *
     * @return the {@code repr}-style representation, or {@code "(none)"} when empty
     */
    public String formatRepr() {
        if (filters.isEmpty()) return "(none)";
        StringBuilder sb = new StringBuilder("PushdownFilters([");
        boolean first = true;
        for (PushdownFilter f : filters) {
            if (!first) sb.append(", ");
            sb.append(formatOneRepr(f));
            first = false;
        }
        sb.append("])");
        return sb.toString();
    }

    private static String formatOneRepr(PushdownFilter f) {
        return switch (f) {
            case PushdownFilter.Constant c -> "ConstantFilter(" + c.columnName() + " "
                    + c.op().symbol() + " " + sqlLiteral(c.value()) + ")";
            case PushdownFilter.IsNull n -> "IsNullFilter(" + n.columnName() + " IS NULL)";
            case PushdownFilter.IsNotNull n -> "IsNotNullFilter(" + n.columnName() + " IS NOT NULL)";
            case PushdownFilter.In in -> {
                if (in.values().size() > 20) {
                    yield "InFilter(" + in.columnName() + " IN (" + in.values().size() + " values))";
                }
                String list = in.values().stream()
                        .map(PushdownFilters::sqlLiteral)
                        .collect(Collectors.joining(", "));
                yield "InFilter(" + in.columnName() + " IN (" + list + "))";
            }
            case PushdownFilter.And a -> {
                List<String> parts = new ArrayList<>();
                for (PushdownFilter c : a.children()) parts.add(formatOneRepr(c));
                yield "ConjunctionAndFilter([" + String.join(", ", parts) + "])";
            }
            case PushdownFilter.Or o -> {
                List<String> parts = new ArrayList<>();
                for (PushdownFilter c : o.children()) parts.add(formatOneRepr(c));
                yield "ConjunctionOrFilter([" + String.join(", ", parts) + "])";
            }
            case PushdownFilter.Struct s -> "StructExtractFilter(" + s.columnName() + "."
                    + s.childName() + " "
                    + (s.childFilter() == null ? "<unfiltered>" : formatOneRepr(s.childFilter()))
                    + ")";
            case PushdownFilter.Expression e -> "ExpressionFilter(" + e.columnName() + ": " + e.sql() + ")";
        };
    }

    private static String formatOne(PushdownFilter f) {
        return switch (f) {
            case PushdownFilter.Constant c -> c.columnName() + " " + c.op().symbol() + " " + sqlLiteral(c.value());
            case PushdownFilter.IsNull n -> n.columnName() + " IS NULL";
            case PushdownFilter.IsNotNull n -> n.columnName() + " IS NOT NULL";
            case PushdownFilter.In in -> {
                if (in.values().size() > 20) {
                    yield in.columnName() + " IN (" + in.values().size() + " values)";
                }
                String list = in.values().stream()
                        .map(PushdownFilters::sqlLiteral)
                        .collect(Collectors.joining(", "));
                yield in.columnName() + " IN (" + list + ")";
            }
            case PushdownFilter.And a -> {
                List<String> parts = new ArrayList<>();
                for (PushdownFilter c : a.children()) parts.add(formatOne(c));
                yield "(" + String.join(" AND ", parts) + ")";
            }
            case PushdownFilter.Or o -> {
                List<String> parts = new ArrayList<>();
                for (PushdownFilter c : o.children()) parts.add(formatOne(c));
                yield "(" + String.join(" OR ", parts) + ")";
            }
            case PushdownFilter.Struct s -> s.columnName() + "." + s.childName() + " " +
                    (s.childFilter() == null ? "<unfiltered>" : formatOne(s.childFilter()));
            case PushdownFilter.Expression e -> e.sql();
        };
    }

    /**
     * The rendered SQL predicates of the top-level expression filters (e.g.
     * {@code ("geom" && ST_MakeEnvelope(...))}). These can't be evaluated
     * row-at-a-time — {@link #evaluate} treats them as pass-through; the worker
     * applies them via an embedded engine (see vgi-example-worker's
     * {@code ExpressionFilterEvaluator}).
     *
     * @return the SQL predicates of the expression filters, in order; empty when none
     */
    public List<String> expressionPredicates() {
        List<String> out = new ArrayList<>();
        for (PushdownFilter f : filters) {
            if (f instanceof PushdownFilter.Expression e) out.add(e.sql());
        }
        return out;
    }

    private static String sqlLiteral(Object v) {
        if (v == null) return "NULL";
        if (v instanceof CharSequence s) return "'" + s + "'";
        if (v instanceof byte[] b) return "'" + new String(b, java.nio.charset.StandardCharsets.UTF_8) + "'";
        return v.toString();
    }

    /**
     * The set of column names referenced by the top-level filters (each
     * filter's {@code column_name}). Mirrors vgi-python's
     * {@code PushdownFilters.filtered_columns}.
     *
     * @return the referenced column names (insertion order; may contain {@code null} for unnamed conjunctions)
     */
    public java.util.Set<String> filteredColumns() {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (PushdownFilter f : filters) out.add(f.columnName());
        return out;
    }

    /**
     * Whether any top-level filter constrains column {@code name}. Mirrors
     * vgi-python's {@code PushdownFilters.has_filter_for_column}.
     *
     * @param name the column name to check
     * @return {@code true} when at least one top-level filter targets {@code name}
     */
    public boolean hasFilterForColumn(String name) {
        for (PushdownFilter f : filters) {
            if (Objects.equals(name, f.columnName())) return true;
        }
        return false;
    }

    /**
     * Return the top-level filters that directly target column {@code name}.
     * "Top-level" means an entry in {@link #filters()} — does not descend into
     * nested {@code And}/{@code Or}/{@code Struct}, since those don't admit a
     * simple per-column extraction. Useful for fixtures that want to know
     * "what bounds did DuckDB push down on column X?" without re-implementing
     * the AST walk.
     *
     * @param name the column name to match
     * @return the matching top-level filters; empty when {@code name} is {@code null} or unmatched
     */
    public List<PushdownFilter> filtersForColumn(String name) {
        if (name == null) return List.of();
        List<PushdownFilter> out = new ArrayList<>();
        for (PushdownFilter f : filters) {
            if (f instanceof PushdownFilter.And || f instanceof PushdownFilter.Or) continue;
            if (name.equals(f.columnName())) out.add(f);
        }
        return out;
    }

    /**
     * Extract the set of values that {@code name} is *definitely* constrained
     * to equal at the top level: collects {@code eq}-Constant values and
     * {@code In} value lists. Returns an empty list when no such constraint
     * exists; callers can treat that as "no direct equality push-down".
     *
     * @param name the column name to inspect
     * @return the equality-constrained values, or an empty list when none were pushed
     */
    public List<Object> directEqualityValues(String name) {
        List<Object> out = new ArrayList<>();
        for (PushdownFilter f : filtersForColumn(name)) {
            switch (f) {
                case PushdownFilter.Constant c -> {
                    if (c.op() == ComparisonOperator.EQ) out.add(c.value());
                }
                case PushdownFilter.In in -> out.addAll(in.values());
                default -> { }
            }
        }
        return out;
    }

    /**
     * Collect the filters constraining {@code name}, descending exactly one
     * level into a top-level {@code And} whose own column is {@code name}
     * (collecting only that And's children that also target {@code name}).
     * Mirrors vgi-python's {@code _collect_column_filters}: DuckDB commonly
     * pushes {@code col = v} / {@code col IN (...)} conjoined with derived range
     * bounds as a single {@code AndFilter}; deeper nesting is not traversed.
     */
    private List<PushdownFilter> collectColumnFilters(String name) {
        List<PushdownFilter> out = new ArrayList<>();
        if (name == null) return out;
        for (PushdownFilter f : filters) {
            if (!name.equals(f.columnName())) continue;
            if (f instanceof PushdownFilter.And a) {
                for (PushdownFilter c : a.children()) {
                    if (name.equals(c.columnName())) out.add(c);
                }
            } else {
                out.add(f);
            }
        }
        return out;
    }

    /**
     * The discrete set of values {@code name} could take, when the pushed
     * filters pin it to an enumerable set — an equality, an {@code IN} list, or
     * an {@code OR} whose every branch pins {@code name} to discrete values
     * (their union). Descends one level into a top-level {@code And} (see
     * {@link #collectColumnFilters}). Returns {@code empty} when not enumerable
     * (no filter, a bare range, an {@code OR} with a range/other-column branch,
     * or deeper nesting) — the caller must then fall back to a full scan rather
     * than an unsafe subset. Mirrors vgi-python's {@code get_column_values}.
     *
     * @param name the column name to inspect
     * @return the enumerable value set, or {@link java.util.Optional#empty()} when not enumerable
     */
    public java.util.Optional<List<Object>> getColumnValues(String name) {
        for (PushdownFilter f : collectColumnFilters(name)) {
            switch (f) {
                case PushdownFilter.Constant c -> {
                    if (c.op() == ComparisonOperator.EQ) {
                        List<Object> one = new ArrayList<>();
                        one.add(c.value());
                        return java.util.Optional.of(one);
                    }
                }
                case PushdownFilter.In in -> {
                    return java.util.Optional.of(new ArrayList<>(in.values()));
                }
                case PushdownFilter.Or o -> {
                    List<Object> union = orDiscreteValues(o, name);
                    if (union != null) return java.util.Optional.of(union);
                }
                default -> { }
            }
        }
        return java.util.Optional.empty();
    }

    /**
     * The deduped union of discrete values for {@code name} across all OR
     * branches, iff every branch pins {@code name} to a discrete {@code =}/
     * {@code IN} value; otherwise {@code null} (a range / {@code IS NULL} /
     * other-column branch leaves the column unbounded, so it is not
     * enumerable). Single-level descent, consistent with the rest of the API.
     */
    private static List<Object> orDiscreteValues(PushdownFilter.Or orFilter, String name) {
        List<Object> values = new ArrayList<>();
        for (PushdownFilter child : orFilter.children()) {
            if (!name.equals(child.columnName())) return null;
            switch (child) {
                case PushdownFilter.Constant c -> {
                    if (c.op() == ComparisonOperator.EQ) values.add(c.value());
                    else return null;
                }
                case PushdownFilter.In in -> values.addAll(in.values());
                default -> { return null; }
            }
        }
        if (values.isEmpty()) return null;
        List<Object> deduped = new ArrayList<>();
        java.util.Set<Object> seen = new java.util.LinkedHashSet<>();
        for (Object v : values) {
            if (seen.add(v)) deduped.add(v);
        }
        return deduped;
    }

    /**
     * Evaluate every filter against {@code root} and return a boolean mask
     * (one entry per row) — {@code true} means the row passes all filters
     * (top-level AND). The mask is a {@code boolean[]} so callers can drive
     * fixture-side row filtering without an Arrow allocation.
     *
     * @param root the batch to evaluate
     * @return a per-row mask where {@code true} means the row passes all filters
     */
    public boolean[] evaluate(VectorSchemaRoot root) {
        int rows = root.getRowCount();
        boolean[] mask = new boolean[rows];
        java.util.Arrays.fill(mask, true);
        for (PushdownFilter f : filters) {
            for (int i = 0; i < rows; i++) {
                if (!mask[i]) continue;
                if (!evalRow(f, root, i)) mask[i] = false;
            }
        }
        return mask;
    }

    private static boolean evalRow(PushdownFilter f, VectorSchemaRoot root, int row) {
        return switch (f) {
            case PushdownFilter.Constant c -> {
                FieldVector v = vec(root, c.columnName(), c.columnIndex());
                if (v == null || v.isNull(row)) yield false;
                Object cell = v.getObject(row);
                yield compare(c.op(), cell, c.value());
            }
            case PushdownFilter.IsNull n -> {
                FieldVector v = vec(root, n.columnName(), n.columnIndex());
                yield v == null || v.isNull(row);
            }
            case PushdownFilter.IsNotNull n -> {
                FieldVector v = vec(root, n.columnName(), n.columnIndex());
                yield v != null && !v.isNull(row);
            }
            case PushdownFilter.In in -> {
                FieldVector v = vec(root, in.columnName(), in.columnIndex());
                if (v == null || v.isNull(row)) yield false;
                Object cell = v.getObject(row);
                for (Object x : in.values()) if (compare(ComparisonOperator.EQ, cell, x)) yield true;
                yield false;
            }
            case PushdownFilter.And a -> {
                for (PushdownFilter c : a.children()) if (!evalRow(c, root, row)) yield false;
                yield true;
            }
            case PushdownFilter.Or o -> {
                for (PushdownFilter c : o.children()) if (evalRow(c, root, row)) yield true;
                yield false;
            }
            case PushdownFilter.Struct s -> {
                FieldVector v = vec(root, s.columnName(), s.columnIndex());
                if (!(v instanceof org.apache.arrow.vector.complex.StructVector sv)) yield true;
                if (sv.isNull(row)) yield false;
                FieldVector child = sv.getChild(s.childName());
                if (child == null) yield true;
                yield evalChildFilter(s.childFilter(), child, row);
            }
            // Expression filters can't be evaluated row-at-a-time; the worker
            // applies them via an embedded engine. Pass-through here.
            case PushdownFilter.Expression e -> true;
        };
    }

    private static boolean evalChildFilter(PushdownFilter f, FieldVector child, int row) {
        if (f == null) return true;
        return switch (f) {
            case PushdownFilter.Constant c -> {
                if (child.isNull(row)) yield false;
                Object cell = child.getObject(row);
                yield compare(c.op(), cell, c.value());
            }
            case PushdownFilter.IsNull n -> child.isNull(row);
            case PushdownFilter.IsNotNull n -> !child.isNull(row);
            case PushdownFilter.In in -> {
                if (child.isNull(row)) yield false;
                Object cell = child.getObject(row);
                for (Object x : in.values()) if (compare(ComparisonOperator.EQ, cell, x)) yield true;
                yield false;
            }
            case PushdownFilter.And a -> {
                for (PushdownFilter cf : a.children()) if (!evalChildFilter(cf, child, row)) yield false;
                yield true;
            }
            case PushdownFilter.Or o -> {
                for (PushdownFilter cf : o.children()) if (evalChildFilter(cf, child, row)) yield true;
                yield false;
            }
            case PushdownFilter.Struct s -> {
                if (!(child instanceof org.apache.arrow.vector.complex.StructVector sv)) yield true;
                if (sv.isNull(row)) yield false;
                FieldVector grandChild = sv.getChild(s.childName());
                yield grandChild == null ? true : evalChildFilter(s.childFilter(), grandChild, row);
            }
            case PushdownFilter.Expression e -> true;
        };
    }

    private static FieldVector vec(VectorSchemaRoot root, String name, int idx) {
        // Prefer name-based lookup: column_index in the wire spec is into
        // DuckDB's projected schema, which doesn't necessarily match the
        // full output schema callers evaluate against.
        if (name != null && !name.isEmpty()) {
            FieldVector v = root.getVector(name);
            if (v != null) return v;
        }
        if (idx >= 0 && idx < root.getFieldVectors().size()) return root.getVector(idx);
        return null;
    }

    private static boolean compare(ComparisonOperator op, Object a, Object b) {
        if (a == null || b == null) return false;
        // Normalise byte[] (Arrow VarBinary/VarChar) to string for comparison.
        if (a instanceof byte[] ab) a = new String(ab, java.nio.charset.StandardCharsets.UTF_8);
        if (b instanceof byte[] bb) b = new String(bb, java.nio.charset.StandardCharsets.UTF_8);
        // Arrow's getObject for VarChar returns Text; coerce both sides to String.
        a = a instanceof org.apache.arrow.vector.util.Text t ? t.toString() : a;
        b = b instanceof org.apache.arrow.vector.util.Text t ? t.toString() : b;
        if (a instanceof Number na && b instanceof Number nb) {
            return op.test(Double.compare(na.doubleValue(), nb.doubleValue()));
        }
        if (a instanceof Comparable && a.getClass() == b.getClass()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            int cmp = ((Comparable) a).compareTo(b);
            return op.test(cmp);
        }
        return op.testEquality(Objects.equals(a, b));
    }
}
