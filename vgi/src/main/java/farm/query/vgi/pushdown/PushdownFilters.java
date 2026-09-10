// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Container for the parsed top-level filter list (implicit AND).
 *
 * Parsed v2 filter snapshot/state, with compatibility views used by existing fixtures.
 */
public final class PushdownFilters {
    private final List<PushdownFilter> filters;
    private final String version;
    private final List<FilterPredicateV2> predicates;
    private final Map<String, Long> revisions;
    private final Set<String> requiredIds;
    private final FilterEvaluationContext evaluationContext;
    private final Schema outputSchema;
    private final List<PushdownFiltersDecoder.ExternalBatch> joinKeys;
    private final PushdownFiltersDecoder.Capabilities capabilities;

    public PushdownFilters(List<PushdownFilter> filters, String version) {
        this(filters, version, List.of(), Map.of(), Set.of(), FilterEvaluationContext.none(),
                null, List.of(), PushdownFiltersDecoder.Capabilities.core());
    }

    private PushdownFilters(
            List<PushdownFilter> filters, String version, List<FilterPredicateV2> predicates,
            Map<String, Long> revisions, Set<String> requiredIds,
            FilterEvaluationContext evaluationContext, Schema outputSchema,
            List<PushdownFiltersDecoder.ExternalBatch> joinKeys,
            PushdownFiltersDecoder.Capabilities capabilities) {
        this.filters = List.copyOf(filters);
        this.version = version;
        this.predicates = List.copyOf(predicates);
        this.revisions = Map.copyOf(revisions);
        this.requiredIds = Set.copyOf(requiredIds);
        this.evaluationContext = evaluationContext;
        this.outputSchema = outputSchema;
        this.joinKeys = List.copyOf(joinKeys);
        this.capabilities = capabilities;
    }

    static PushdownFilters v2(
            List<FilterPredicateV2> predicates, Map<String, Long> revisions,
            Set<String> requiredIds, FilterEvaluationContext evaluationContext,
            Schema outputSchema, List<PushdownFiltersDecoder.ExternalBatch> joinKeys,
            PushdownFiltersDecoder.Capabilities capabilities) {
        List<PushdownFilter> views = new ArrayList<>();
        for (FilterPredicateV2 predicate : predicates) {
            views.add(compatibilityView(predicate.expression()));
        }
        return new PushdownFilters(views, "2", predicates, revisions, requiredIds,
                evaluationContext, outputSchema, joinKeys, capabilities);
    }

    public List<PushdownFilter> filters() { return filters; }
    public String version() { return version; }
    public List<FilterPredicateV2> predicates() { return predicates; }
    public Map<String, Long> revisions() { return revisions; }
    public Set<String> requiredIds() { return requiredIds; }
    public FilterEvaluationContext evaluationContext() { return evaluationContext; }
    Schema outputSchema() { return outputSchema; }
    List<PushdownFiltersDecoder.ExternalBatch> joinKeys() { return joinKeys; }
    PushdownFiltersDecoder.Capabilities capabilities() { return capabilities; }

    /**
     * An empty filter set at the current supported version.
     *
     * @return a {@code PushdownFilters} with no filters
     */
    public static PushdownFilters empty() {
        return new PushdownFilters(List.of(), "2");
    }

    /** Atomically apply one tick-time advisory delta to this scan state. */
    public PushdownFilters applyDelta(byte[] delta) {
        return PushdownFiltersDecoder.applyDelta(this, delta);
    }

    /** Conjoin two initial/refinement snapshots without losing their typed v2 state. */
    public PushdownFilters mergeSnapshot(PushdownFilters refinement) {
        if (refinement == null) return this;
        if (!version.equals("2") || !refinement.version.equals("2")) {
            throw new FilterV2Exception("only v2 snapshots can be merged");
        }
        if (!evaluationContext.equals(refinement.evaluationContext)) {
            throw new FilterV2Exception("refinement changed the evaluation context");
        }
        Map<String, Long> mergedRevisions = new LinkedHashMap<>(revisions);
        Map<String, String> renamed = new LinkedHashMap<>();
        for (var entry : refinement.revisions.entrySet()) {
            String id = entry.getKey();
            while (mergedRevisions.containsKey(id)) id = "refinement:" + id;
            renamed.put(entry.getKey(), id);
            mergedRevisions.put(id, entry.getValue());
        }
        List<FilterPredicateV2> mergedPredicates = new ArrayList<>(predicates);
        for (FilterPredicateV2 predicate : refinement.predicates) {
            mergedPredicates.add(new FilterPredicateV2(renamed.get(predicate.id()), predicate.revision(),
                    predicate.mode(), predicate.source(), predicate.expression()));
        }
        Set<String> mergedRequired = new java.util.LinkedHashSet<>(requiredIds);
        for (String id : refinement.requiredIds) mergedRequired.add(renamed.get(id));
        List<PushdownFiltersDecoder.ExternalBatch> mergedKeys = new ArrayList<>(joinKeys);
        mergedKeys.addAll(refinement.joinKeys);
        return v2(mergedPredicates, mergedRevisions, mergedRequired, evaluationContext,
                outputSchema == null ? refinement.outputSchema : outputSchema,
                mergedKeys, capabilities);
    }

    private static PushdownFilter compatibilityView(FilterExpression expression) {
        if (expression instanceof FilterExpression.Comparison comparison) {
            Path path = path(comparison.left());
            FilterExpression.Literal literal = comparison.right() instanceof FilterExpression.Literal value
                    ? value : null;
            if (path == null || literal == null) {
                path = path(comparison.right());
                literal = comparison.left() instanceof FilterExpression.Literal value ? value : null;
            }
            if (path != null && literal != null) {
                return wrap(path, new PushdownFilter.Constant(
                        path.leafName(), path.leafIndex(), comparison.op(), literal.value()));
            }
        }
        if (expression instanceof FilterExpression.IsNull isNull) {
            Path path = path(isNull.expression());
            if (path != null) {
                PushdownFilter leaf = isNull.negated()
                        ? new PushdownFilter.IsNotNull(path.leafName(), path.leafIndex())
                        : new PushdownFilter.IsNull(path.leafName(), path.leafIndex());
                return wrap(path, leaf);
            }
        }
        if (expression instanceof FilterExpression.In in) {
            Path path = path(in.expression());
            if (path != null && !in.negated()) {
                List<Object> values = in.set() instanceof FilterExpression.LiteralSet set
                        ? set.values() : ((FilterExpression.ExternalSet) in.set()).values();
                return wrap(path, new PushdownFilter.In(path.leafName(), path.leafIndex(), values));
            }
        }
        if (expression instanceof FilterExpression.BooleanExpression booleanExpression) {
            List<PushdownFilter> children = booleanExpression.children().stream()
                    .map(PushdownFilters::compatibilityView).toList();
            Path first = firstPath(expression);
            String name = first == null ? null : first.rootName();
            int index = first == null ? -1 : first.rootIndex();
            return booleanExpression.conjunction()
                    ? new PushdownFilter.And(name, index, children)
                    : new PushdownFilter.Or(name, index, children);
        }
        Path first = firstPath(expression);
        return new PushdownFilter.Expression(first == null ? null : first.rootName(),
                first == null ? -1 : first.rootIndex(), expressionSql(expression));
    }

    private record Path(String rootName, int rootIndex, List<PathPart> parts) {
        String leafName() { return parts.isEmpty() ? rootName : parts.getLast().name(); }
        int leafIndex() { return parts.isEmpty() ? rootIndex : parts.getLast().index(); }
    }

    private record PathPart(String name, int index) {}

    private static Path path(FilterExpression expression) {
        if (expression instanceof FilterExpression.ColumnRef column) {
            return new Path(column.columnName(), Math.toIntExact(column.columnIndex()), List.of());
        }
        if (expression instanceof FilterExpression.FieldRef field) {
            Path parent = path(field.expression());
            if (parent == null) return null;
            List<PathPart> parts = new ArrayList<>(parent.parts());
            parts.add(new PathPart(field.fieldName(), Math.toIntExact(field.fieldIndex())));
            return new Path(parent.rootName(), parent.rootIndex(), List.copyOf(parts));
        }
        return null;
    }

    private static PushdownFilter wrap(Path path, PushdownFilter leaf) {
        PushdownFilter result = leaf;
        for (int i = path.parts().size() - 1; i >= 0; i--) {
            PathPart part = path.parts().get(i);
            String parentName = i == 0 ? path.rootName() : path.parts().get(i - 1).name();
            int parentIndex = i == 0 ? path.rootIndex() : path.parts().get(i - 1).index();
            result = new PushdownFilter.Struct(parentName, parentIndex, part.index(), part.name(), result);
        }
        return result;
    }

    private static Path firstPath(FilterExpression expression) {
        Path direct = path(expression);
        if (direct != null) return direct;
        return switch (expression) {
            case FilterExpression.Comparison value -> firstNonNull(firstPath(value.left()), firstPath(value.right()));
            case FilterExpression.BooleanExpression value -> value.children().stream()
                    .map(PushdownFilters::firstPath).filter(Objects::nonNull).findFirst().orElse(null);
            case FilterExpression.Not value -> firstPath(value.expression());
            case FilterExpression.IsNull value -> firstPath(value.expression());
            case FilterExpression.In value -> firstPath(value.expression());
            case FilterExpression.Cast value -> firstPath(value.expression());
            case FilterExpression.Arithmetic value -> firstNonNull(firstPath(value.left()), firstPath(value.right()));
            case FilterExpression.Negate value -> firstPath(value.expression());
            case FilterExpression.Call value -> value.arguments().stream()
                    .map(PushdownFilters::firstPath).filter(Objects::nonNull).findFirst().orElse(null);
            case FilterExpression.RuntimeFilter value -> firstPath(value.input());
            default -> null;
        };
    }

    private static Path firstNonNull(Path left, Path right) { return left == null ? right : left; }

    private static String expressionSql(FilterExpression expression) {
        return switch (expression) {
            case FilterExpression.ColumnRef value -> quoteIdentifier(value.columnName());
            case FilterExpression.FieldRef value -> expressionSql(value.expression()) + "."
                    + quoteIdentifier(value.fieldName());
            case FilterExpression.Literal value -> sqlLiteral(value.value());
            case FilterExpression.Comparison value -> "(" + expressionSql(value.left()) + " "
                    + value.op().symbol() + " " + expressionSql(value.right()) + ")";
            case FilterExpression.BooleanExpression value -> "(" + value.children().stream()
                    .map(PushdownFilters::expressionSql)
                    .collect(Collectors.joining(value.conjunction() ? " AND " : " OR ")) + ")";
            case FilterExpression.Not value -> "(NOT " + expressionSql(value.expression()) + ")";
            case FilterExpression.IsNull value -> "(" + expressionSql(value.expression())
                    + (value.negated() ? " IS NOT NULL)" : " IS NULL)");
            case FilterExpression.In value -> "(" + expressionSql(value.expression())
                    + (value.negated() ? " NOT IN (...)" : " IN (...)") + ")";
            case FilterExpression.Cast value -> "CAST(" + expressionSql(value.expression()) + " AS "
                    + value.field().getType() + ")";
            case FilterExpression.Arithmetic value -> "(" + expressionSql(value.left()) + " "
                    + arithmeticSymbol(value.op()) + " " + expressionSql(value.right()) + ")";
            case FilterExpression.Negate value -> "(-" + expressionSql(value.expression()) + ")";
            case FilterExpression.Call value -> functionName(value.function()) + "("
                    + value.arguments().stream().map(PushdownFilters::expressionSql)
                    .collect(Collectors.joining(", ")) + ")";
            case FilterExpression.RuntimeFilter value -> "runtime_filter(" + expressionSql(value.input()) + ")";
        };
    }

    private static String quoteIdentifier(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String arithmeticSymbol(FilterExpression.ArithmeticOperator op) {
        return switch (op) {
            case ADD -> "+";
            case SUBTRACT -> "-";
            case MULTIPLY -> "*";
            case DIVIDE -> "/";
            case MODULO -> "%";
        };
    }

    private static String functionName(Object function) {
        if (function instanceof FilterExpression.StandardFunction standard) {
            return standard.name().toLowerCase();
        }
        FilterIdentity identity = (FilterIdentity) function;
        if (identity.equals(new FilterIdentity("duckdb.spatial", "intersects_extent", 1))) return "&&";
        return identity.namespace() + "." + identity.name();
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
        if (!predicates.isEmpty()) {
            for (FilterPredicateV2 predicate : predicates) {
                if (predicate.expression() instanceof FilterExpression.RuntimeFilter runtime
                        && !runtime.supported()) continue;
                boolean[] candidate = mask.clone();
                try {
                    for (int row = 0; row < rows; row++) {
                        if (!candidate[row]) continue;
                        if (truth(predicate.expression(), root, row) != Truth.TRUE) mask[row] = false;
                    }
                } catch (RuntimeException e) {
                    if (predicate.mode() == PredicateMode.REQUIRED) {
                        throw new FilterV2Exception(
                                "failed to evaluate required predicate " + predicate.id(), e);
                    }
                    mask = candidate;
                }
            }
            return mask;
        }
        for (PushdownFilter f : filters) {
            for (int i = 0; i < rows; i++) {
                if (!mask[i]) continue;
                if (!evalRow(f, root, i)) mask[i] = false;
            }
        }
        return mask;
    }

    private enum Truth {
        TRUE, FALSE, NULL;

        Truth not() { return this == TRUE ? FALSE : this == FALSE ? TRUE : NULL; }
    }

    private record Scalar(Object value, Field field) {}
    private record StructCell(org.apache.arrow.vector.complex.StructVector vector, int row) {}

    private static Truth truth(FilterExpression expression, VectorSchemaRoot root, int row) {
        if (expression instanceof FilterExpression.Comparison comparison) {
            Scalar left = scalar(comparison.left(), root, row);
            Scalar right = scalar(comparison.right(), root, row);
            if (comparison.op() == ComparisonOperator.DISTINCT_FROM) {
                if (left.value() == null || right.value() == null) {
                    return left.value() == right.value() ? Truth.FALSE : Truth.TRUE;
                }
            } else if (comparison.op() == ComparisonOperator.NOT_DISTINCT_FROM) {
                if (left.value() == null || right.value() == null) {
                    return left.value() == right.value() ? Truth.TRUE : Truth.FALSE;
                }
            } else if (left.value() == null || right.value() == null) {
                return Truth.NULL;
            }
            return compare(comparison.op(), left.value(), right.value()) ? Truth.TRUE : Truth.FALSE;
        }
        if (expression instanceof FilterExpression.BooleanExpression bool) {
            Truth result = bool.conjunction() ? Truth.TRUE : Truth.FALSE;
            for (FilterExpression child : bool.children()) {
                Truth value = truth(child, root, row);
                if (bool.conjunction()) {
                    if (value == Truth.FALSE) return Truth.FALSE;
                    if (value == Truth.NULL) result = Truth.NULL;
                } else {
                    if (value == Truth.TRUE) return Truth.TRUE;
                    if (value == Truth.NULL) result = Truth.NULL;
                }
            }
            return result;
        }
        if (expression instanceof FilterExpression.Not not) return truth(not.expression(), root, row).not();
        if (expression instanceof FilterExpression.IsNull isNull) {
            boolean result = scalar(isNull.expression(), root, row).value() == null;
            return result != isNull.negated() ? Truth.TRUE : Truth.FALSE;
        }
        if (expression instanceof FilterExpression.In in) {
            List<Object> values = in.set() instanceof FilterExpression.LiteralSet set
                    ? set.values() : ((FilterExpression.ExternalSet) in.set()).values();
            if (values.isEmpty()) return in.negated() ? Truth.TRUE : Truth.FALSE;
            Object input = scalar(in.expression(), root, row).value();
            if (input == null) return Truth.NULL;
            boolean sawNull = false;
            for (Object value : values) {
                if (value == null) sawNull = true;
                else if (compare(ComparisonOperator.EQ, input, value)) {
                    return in.negated() ? Truth.FALSE : Truth.TRUE;
                }
            }
            Truth result = sawNull ? Truth.NULL : Truth.FALSE;
            return in.negated() ? result.not() : result;
        }
        if (expression instanceof FilterExpression.Call call) {
            Object value = scalar(call, root, row).value();
            return value == null ? Truth.NULL : (Boolean) value ? Truth.TRUE : Truth.FALSE;
        }
        if (expression instanceof FilterExpression.Literal literal
                && literal.field().getType() instanceof ArrowType.Bool) {
            return literal.value() == null ? Truth.NULL
                    : (Boolean) literal.value() ? Truth.TRUE : Truth.FALSE;
        }
        if (expression instanceof FilterExpression.ColumnRef
                || expression instanceof FilterExpression.FieldRef
                || expression instanceof FilterExpression.Cast) {
            Scalar value = scalar(expression, root, row);
            if (!(value.field().getType() instanceof ArrowType.Bool)) {
                throw new FilterV2Exception("predicate root does not resolve to BOOLEAN");
            }
            return value.value() == null ? Truth.NULL
                    : (Boolean) value.value() ? Truth.TRUE : Truth.FALSE;
        }
        if (expression instanceof FilterExpression.RuntimeFilter runtime) {
            if (!runtime.supported()) return Truth.TRUE;
            throw new FilterV2Exception("runtime-filter evaluator is unavailable");
        }
        throw new FilterV2Exception("expression does not resolve to BOOLEAN");
    }

    private static Scalar scalar(FilterExpression expression, VectorSchemaRoot root, int row) {
        if (expression instanceof FilterExpression.ColumnRef column) {
            FieldVector vector = resolve(root, column.columnName(), column.columnIndex());
            Field actual = vector.getField();
            if (column.field() != null && !column.field().getType().equals(actual.getType())) {
                throw new FilterV2Exception("referenced column changed type before evaluation");
            }
            Object value = vector.isNull(row) ? null
                    : vector instanceof org.apache.arrow.vector.complex.StructVector struct
                            ? new StructCell(struct, row)
                            : farm.query.vgi.internal.VectorScalarCodec.read(vector, row);
            return new Scalar(value, actual);
        }
        if (expression instanceof FilterExpression.FieldRef field) {
            Scalar parent = scalar(field.expression(), root, row);
            if (!(parent.field().getType() instanceof ArrowType.Struct)) {
                throw new FilterV2Exception("field_ref input is not a struct");
            }
            if (field.fieldIndex() >= parent.field().getChildren().size()) {
                throw new FilterV2Exception("field_ref index is out of range");
            }
            Field child = parent.field().getChildren().get((int) field.fieldIndex());
            if (!child.getName().equals(field.fieldName())) {
                throw new FilterV2Exception("field_ref name does not match its index");
            }
            if (parent.value() == null) return new Scalar(null, child);
            if (parent.value() instanceof StructCell cell) {
                FieldVector childVector = (FieldVector) cell.vector()
                        .getChildByOrdinal((int) field.fieldIndex());
                if (childVector == null || !childVector.getName().equals(field.fieldName())) {
                    throw new FilterV2Exception("field_ref child vector does not match its index and name");
                }
                Object value = childVector.isNull(cell.row()) ? null
                        : childVector instanceof org.apache.arrow.vector.complex.StructVector struct
                                ? new StructCell(struct, cell.row())
                                : farm.query.vgi.internal.VectorScalarCodec.read(childVector, cell.row());
                return new Scalar(value, child);
            }
            if (!(parent.value() instanceof Map<?, ?> values)) {
                throw new FilterV2Exception("field_ref input value is not a struct");
            }
            return new Scalar(values.get(field.fieldName()), child);
        }
        if (expression instanceof FilterExpression.Literal literal) {
            return new Scalar(literal.value(), literal.field());
        }
        if (expression instanceof FilterExpression.Cast cast) {
            Scalar input = scalar(cast.expression(), root, row);
            return new Scalar(cast(input.value(), cast.field().getType()), cast.field());
        }
        if (expression instanceof FilterExpression.Arithmetic arithmetic) {
            Scalar left = scalar(arithmetic.left(), root, row);
            Scalar right = scalar(arithmetic.right(), root, row);
            return new Scalar(arithmetic(arithmetic.op(), left.value(), right.value()), left.field());
        }
        if (expression instanceof FilterExpression.Negate negate) {
            Scalar value = scalar(negate.expression(), root, row);
            return new Scalar(value.value() == null ? null : decimal(value.value()).negate(), value.field());
        }
        if (expression instanceof FilterExpression.Call call) {
            List<Object> args = call.arguments().stream().map(arg -> scalar(arg, root, row).value()).toList();
            if (!(call.function() instanceof FilterExpression.StandardFunction function)) {
                return new Scalar(true, new Field("result",
                        org.apache.arrow.vector.types.pojo.FieldType.nullable(new ArrowType.Bool()), null));
            }
            Object result = switch (function) {
                case STARTS_WITH -> stringCall(args, String::startsWith);
                case ENDS_WITH -> stringCall(args, String::endsWith);
                case CONTAINS -> stringCall(args, String::contains);
                case LIST_CONTAINS -> listContains(args);
            };
            return new Scalar(result, new Field("result", org.apache.arrow.vector.types.pojo.FieldType.nullable(
                    new ArrowType.Bool()), null));
        }
        throw new FilterV2Exception("expression cannot be used as a scalar");
    }

    private static FieldVector resolve(VectorSchemaRoot root, String name, long index) {
        if (index < root.getFieldVectors().size()
                && root.getSchema().getFields().get((int) index).getName().equals(name)) {
            return root.getVector((int) index);
        }
        List<FieldVector> matches = root.getFieldVectors().stream()
                .filter(vector -> vector.getName().equals(name)).toList();
        if (matches.size() != 1) throw new FilterV2Exception("referenced column is unavailable or ambiguous: " + name);
        return matches.getFirst();
    }

    private static Object cast(Object value, ArrowType target) {
        if (value == null) return null;
        if (target instanceof ArrowType.Utf8 || target instanceof ArrowType.LargeUtf8) return value.toString();
        if (target instanceof ArrowType.Bool) {
            if (value instanceof Boolean b) return b;
            if (value.toString().equalsIgnoreCase("true")) return true;
            if (value.toString().equalsIgnoreCase("false")) return false;
            throw new FilterV2Exception("invalid BOOLEAN cast");
        }
        if (target instanceof ArrowType.Int integer) {
            BigDecimal number = decimal(value);
            long result = number.longValueExact();
            int bits = integer.getBitWidth();
            if (integer.getIsSigned()) {
                if (bits < 64) {
                    long min = -(1L << (bits - 1));
                    long max = (1L << (bits - 1)) - 1;
                    if (result < min || result > max) throw new ArithmeticException("integer cast overflow");
                }
            } else if (result < 0 || (bits < 63 && result >= (1L << bits))) {
                throw new ArithmeticException("unsigned integer cast overflow");
            }
            return result;
        }
        if (target instanceof ArrowType.FloatingPoint) return Double.valueOf(value.toString());
        if (target instanceof ArrowType.Decimal decimal) {
            return PushdownFilters.decimal(value).setScale(decimal.getScale());
        }
        throw new FilterV2Exception("unsupported cast target " + target);
    }

    private static Object arithmetic(FilterExpression.ArithmeticOperator op, Object left, Object right) {
        if (left == null || right == null) return null;
        BigDecimal a = decimal(left);
        BigDecimal b = decimal(right);
        return switch (op) {
            case ADD -> a.add(b);
            case SUBTRACT -> a.subtract(b);
            case MULTIPLY -> a.multiply(b);
            case DIVIDE -> a.divide(b, MathContext.DECIMAL128);
            case MODULO -> a.remainder(b);
        };
    }

    private static BigDecimal decimal(Object value) {
        return value instanceof BigDecimal decimal ? decimal : new BigDecimal(value.toString());
    }

    private static Object stringCall(List<Object> args,
                                     java.util.function.BiPredicate<String, String> operation) {
        if (args.size() != 2) throw new FilterV2Exception("string function requires two arguments");
        if (args.get(0) == null || args.get(1) == null) return null;
        return operation.test(args.get(0).toString(), args.get(1).toString());
    }

    private static Object listContains(List<Object> args) {
        if (args.size() != 2) throw new FilterV2Exception("list_contains requires two arguments");
        if (args.get(0) == null || args.get(1) == null) return null;
        if (!(args.get(0) instanceof List<?> list)) throw new FilterV2Exception("list_contains input is not a list");
        for (Object value : list) if (Objects.equals(value, args.get(1))) return true;
        return false;
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
        if (a instanceof byte[] left && b instanceof byte[] right) {
            int cmp = java.util.Arrays.compareUnsigned(left, right);
            return op.test(cmp);
        }
        a = a instanceof org.apache.arrow.vector.util.Text t ? t.toString() : a;
        b = b instanceof org.apache.arrow.vector.util.Text t ? t.toString() : b;
        if (a instanceof Number && b instanceof Number) {
            if (a instanceof Float || a instanceof Double || b instanceof Float || b instanceof Double) {
                return op.test(Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()));
            }
            return op.test(decimal(a).compareTo(decimal(b)));
        }
        if (a instanceof Comparable && a.getClass() == b.getClass()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            int cmp = ((Comparable) a).compareTo(b);
            return op.test(cmp);
        }
        return op.testEquality(Objects.equals(a, b));
    }
}
