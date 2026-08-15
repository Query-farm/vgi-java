// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.pushdown.ComparisonOperator;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.ArrayList;
import java.util.List;

/**
 * A pushdown predicate, <em>without</em> the column it applies to.
 *
 * <p>The wire form repeats {@code column_name} / {@code column_index} on every
 * node, including the children of an {@code and}/{@code or} and the
 * {@code child_filter} of a {@code struct} — DuckDB's serializer copies the
 * parent's column identity down the tree, because a pushed filter is always
 * rooted at exactly one column. Modelling the predicate separately from the
 * column keeps that invariant structural instead of clerical: you attach a
 * column once, in
 * {@link PushdownFiltersEncoder#filter(ProjectedColumn, FilterPredicate)}, and
 * the encoder stamps it onto every node it emits.
 *
 * <p>Build predicates through the static factories:
 *
 * <pre>{@code
 * FilterPredicate.and(FilterPredicate.ge(5L), FilterPredicate.lt(100L));
 * FilterPredicate.or(FilterPredicate.isNull(), FilterPredicate.eq("x"));
 * FilterPredicate.structField(1, "city", FilterPredicate.eq("Berlin"));
 * FilterPredicate.joinKeys(List.of(1L, 2L, 3L));
 * }</pre>
 */
public sealed interface FilterPredicate {

    /**
     * {@code column OP constant}.
     *
     * @param op    the comparison operator
     * @param value the constant operand, written into a sibling {@code _val_N} column
     */
    record Compare(ComparisonOperator op, ScalarValue value) implements FilterPredicate {}

    /** {@code column IS NULL}. */
    record IsNull() implements FilterPredicate {}

    /** {@code column IS NOT NULL}. */
    record IsNotNull() implements FilterPredicate {}

    /**
     * Conjunction of child predicates, all on the same column.
     *
     * @param children the conjoined predicates
     */
    record And(List<FilterPredicate> children) implements FilterPredicate {}

    /**
     * Disjunction of child predicates, all on the same column.
     *
     * @param children the disjoined predicates
     */
    record Or(List<FilterPredicate> children) implements FilterPredicate {}

    /**
     * A predicate that recurses into one field of a struct column.
     *
     * @param childIndex  the struct child's index within the struct
     * @param childName   the struct child's name
     * @param childFilter the predicate applied to that child
     */
    record StructField(int childIndex, String childName, FilterPredicate childFilter)
            implements FilterPredicate {}

    /**
     * Set membership whose values travel <em>out of band</em>: the wire node
     * carries only a {@code keys_column} name, and the values ride as their own
     * single-column batch in {@code InitRequest.join_keys}, matched back to the
     * node by that name. This is the shape a runtime join-key pushdown takes —
     * a build side's distinct keys handed to the scan.
     *
     * @param type   the Arrow type of every key
     * @param values the key values
     */
    record JoinKeys(ArrowType type, List<Object> values) implements FilterPredicate {}

    /**
     * {@code column = value}.
     *
     * @param value the constant, as a {@link ScalarValue} or a value with an inferable type
     * @return the predicate
     */
    static FilterPredicate eq(Object value) {
        return compare(ComparisonOperator.EQ, value);
    }

    /**
     * {@code column != value}.
     *
     * @param value the constant, as a {@link ScalarValue} or a value with an inferable type
     * @return the predicate
     */
    static FilterPredicate ne(Object value) {
        return compare(ComparisonOperator.NE, value);
    }

    /**
     * {@code column > value}.
     *
     * @param value the constant, as a {@link ScalarValue} or a value with an inferable type
     * @return the predicate
     */
    static FilterPredicate gt(Object value) {
        return compare(ComparisonOperator.GT, value);
    }

    /**
     * {@code column >= value}.
     *
     * @param value the constant, as a {@link ScalarValue} or a value with an inferable type
     * @return the predicate
     */
    static FilterPredicate ge(Object value) {
        return compare(ComparisonOperator.GE, value);
    }

    /**
     * {@code column < value}.
     *
     * @param value the constant, as a {@link ScalarValue} or a value with an inferable type
     * @return the predicate
     */
    static FilterPredicate lt(Object value) {
        return compare(ComparisonOperator.LT, value);
    }

    /**
     * {@code column <= value}.
     *
     * @param value the constant, as a {@link ScalarValue} or a value with an inferable type
     * @return the predicate
     */
    static FilterPredicate le(Object value) {
        return compare(ComparisonOperator.LE, value);
    }

    /**
     * {@code column OP value} for an explicitly chosen operator.
     *
     * @param op    the comparison operator
     * @param value the constant, as a {@link ScalarValue} or a value with an inferable type
     * @return the predicate
     */
    static FilterPredicate compare(ComparisonOperator op, Object value) {
        if (op == null) throw new IllegalArgumentException("comparison operator must not be null");
        return new Compare(op, ScalarValue.of(value));
    }

    /**
     * {@code column IS NULL}.
     *
     * @return the predicate
     */
    static FilterPredicate isNull() {
        return new IsNull();
    }

    /**
     * {@code column IS NOT NULL}.
     *
     * @return the predicate
     */
    static FilterPredicate isNotNull() {
        return new IsNotNull();
    }

    /**
     * Conjunction of predicates on the same column.
     *
     * @param children the conjoined predicates; at least one
     * @return the predicate
     */
    static FilterPredicate and(FilterPredicate... children) {
        return new And(requireChildren("and", children));
    }

    /**
     * Disjunction of predicates on the same column.
     *
     * @param children the disjoined predicates; at least one
     * @return the predicate
     */
    static FilterPredicate or(FilterPredicate... children) {
        return new Or(requireChildren("or", children));
    }

    /**
     * Recurse into one field of a struct column.
     *
     * @param childIndex  the struct child's index within the struct
     * @param childName   the struct child's name
     * @param childFilter the predicate applied to that child
     * @return the predicate
     */
    static FilterPredicate structField(int childIndex, String childName,
                                       FilterPredicate childFilter) {
        if (childName == null || childName.isEmpty()) {
            throw new IllegalArgumentException("struct child requires a non-empty name");
        }
        if (childFilter == null) {
            throw new IllegalArgumentException("struct child requires a child filter");
        }
        return new StructField(childIndex, childName, childFilter);
    }

    /**
     * Membership against out-of-band join keys, with the key type inferred from
     * the first value.
     *
     * @param values the key values; must be non-empty and share one Arrow type
     * @return the predicate
     */
    static FilterPredicate joinKeys(List<?> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException(
                    "join keys require at least one value — an empty key set is not pushable");
        }
        return joinKeys(ScalarValue.of(values.get(0)).type(), values);
    }

    /**
     * Membership against out-of-band join keys, written as an explicit type.
     *
     * @param type   the Arrow type of every key
     * @param values the key values; {@code null} entries are written as null keys
     * @return the predicate
     */
    static FilterPredicate joinKeys(ArrowType type, List<?> values) {
        if (type == null) throw new IllegalArgumentException("join keys require an Arrow type");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException(
                    "join keys require at least one value — an empty key set is not pushable");
        }
        // Not List.copyOf: a null key is legal on the wire and copyOf rejects nulls.
        return new JoinKeys(type, java.util.Collections.unmodifiableList(new ArrayList<>(values)));
    }

    private static List<FilterPredicate> requireChildren(String kind, FilterPredicate... children) {
        if (children == null || children.length == 0) {
            throw new IllegalArgumentException(kind + " requires at least one child predicate");
        }
        List<FilterPredicate> out = new ArrayList<>(children.length);
        for (FilterPredicate c : children) {
            if (c == null) throw new IllegalArgumentException(kind + " child must not be null");
            out.add(c);
        }
        return List.copyOf(out);
    }
}
