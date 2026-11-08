// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import farm.query.vgi.pushdown.ComparisonOperator;
import farm.query.vgi.pushdown.FilterExpression;
import farm.query.vgi.pushdown.FilterPredicateV2;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The query predicates sent with a protocol 3.2 filtered
 * {@code catalog_table_scan_branches_get} ({@code pushdown_filters}).
 *
 * <p>The raw bytes are an Arrow IPC stream holding one VGI Filter Encoding v2
 * snapshot batch whose column indexes refer to the table's declared columns
 * ({@code TableInfo.columns}). {@link #decode()} decodes them against those
 * columns on first use and caches the result.</p>
 *
 * <p>Every predicate is advisory: the client re-applies all of them, so a
 * planner may ignore any predicate it does not understand, but must never drop
 * a branch or file that could hold a matching row. {@link #mayMatch} prunes a
 * candidate described by min/max statistics. Mirrors vgi-python
 * {@code vgi.scan_planning.ScanPlanningFilters}; see
 * {@code docs/protocol/scan-planning.md}.</p>
 */
public final class ScanPlanningFilters {

    private final byte[] raw;
    private final Supplier<Schema> tableColumns;
    private PushdownFilters decoded;

    /**
     * Wrap the wire bytes.
     *
     * @param raw          the {@code pushdown_filters} bytes; never {@code null}
     * @param tableColumns resolves the table's declared columns (the schema the
     *                     column indexes refer to); called at most once, on first decode
     */
    public ScanPlanningFilters(byte[] raw, Supplier<Schema> tableColumns) {
        this.raw = Objects.requireNonNull(raw, "raw").clone();
        this.tableColumns = Objects.requireNonNull(tableColumns, "tableColumns");
    }

    /**
     * Wrap wire bytes whose declared columns are already known.
     *
     * @param raw          the {@code pushdown_filters} bytes
     * @param tableColumns the table's declared columns
     * @return the filters
     */
    public static ScanPlanningFilters of(byte[] raw, Schema tableColumns) {
        return new ScanPlanningFilters(raw, () -> tableColumns);
    }

    /**
     * The wire bytes, as received.
     *
     * @return a copy of the {@code pushdown_filters} IPC stream bytes
     */
    public byte[] raw() {
        return raw.clone();
    }

    /**
     * Decode the predicates against the table's declared columns (cached).
     *
     * @return the decoded v2 snapshot
     * @throws farm.query.vgi.pushdown.FilterV2Exception if the bytes are not a valid snapshot
     */
    public synchronized PushdownFilters decode() {
        if (decoded == null) {
            decoded = PushdownFiltersDecoder.decode(raw, tableColumns.get(), List.of(),
                    PushdownFiltersDecoder.Capabilities.core());
        }
        return decoded;
    }

    /**
     * Whether the predicates may match some row of a candidate (a file, a
     * partition, a branch) described by {@code statistics}.
     *
     * <p>{@code statistics} makes the same conservative claims as
     * {@code ScanBranch.columnStatistics}: every non-null value of a column lies
     * within {@code [min, max]}, {@code hasNull == false} means no NULLs, and
     * {@code hasNotNull == false} means the column is entirely NULL. Returns
     * {@code false} only when some predicate is provably false for every row;
     * columns, predicates, or types this helper cannot reason about count as
     * "may match", so the answer errs towards keeping data.</p>
     *
     * @param statistics per-column statistics of the candidate, keyed by declared column name
     * @return {@code false} when the candidate provably holds no matching row
     */
    public boolean mayMatch(Iterable<ColumnStatistics> statistics) {
        Map<String, ColumnStatistics> byName = new HashMap<>();
        for (ColumnStatistics s : statistics) {
            if (s != null && s.columnName() != null) byName.put(s.columnName(), s);
        }
        if (byName.isEmpty()) return true;
        for (FilterPredicateV2 predicate : decode().predicates()) {
            if (excludes(predicate.expression(), byName)) return false;
        }
        return true;
    }

    /** Whether {@code e} is provably false (or NULL) for every row the statistics describe. */
    private static boolean excludes(FilterExpression e, Map<String, ColumnStatistics> stats) {
        return switch (e) {
            case FilterExpression.BooleanExpression b -> {
                if (b.conjunction()) {
                    yield b.children().stream().anyMatch(c -> excludes(c, stats));
                }
                yield !b.children().isEmpty() && b.children().stream().allMatch(c -> excludes(c, stats));
            }
            case FilterExpression.Comparison c -> comparisonExcludes(c, stats);
            case FilterExpression.In in -> inExcludes(in, stats);
            case FilterExpression.IsNull n -> {
                ColumnStatistics s = n.expression() instanceof FilterExpression.ColumnRef col
                        ? stats.get(col.columnName()) : null;
                if (s == null) yield false;
                yield n.negated() ? !s.hasNotNull() : !s.hasNull();
            }
            default -> false;
        };
    }

    private static boolean comparisonExcludes(FilterExpression.Comparison c, Map<String, ColumnStatistics> stats) {
        FilterExpression.ColumnRef col;
        FilterExpression.Literal lit;
        ComparisonOperator op = c.op();
        if (c.left() instanceof FilterExpression.ColumnRef l && c.right() instanceof FilterExpression.Literal r) {
            col = l;
            lit = r;
        } else if (c.right() instanceof FilterExpression.ColumnRef r
                && c.left() instanceof FilterExpression.Literal l) {
            col = r;
            lit = l;
            op = flip(op);
        } else {
            return false;
        }
        ColumnStatistics s = stats.get(col.columnName());
        if (s == null || lit.value() == null || !comparableTypes(col, lit.field().getType())) return false;
        if (!s.hasNotNull()) {
            return op != ComparisonOperator.DISTINCT_FROM;
        }
        Object v = lit.value();
        Object lo = s.min();
        Object hi = s.max();
        return switch (op) {
            case EQ, NOT_DISTINCT_FROM -> outside(v, lo, hi);
            case GT -> atMost(hi, v);
            case GE -> less(hi, v);
            case LT -> atMost(v, lo);
            case LE -> less(v, lo);
            case NE -> eq(lo, v) && eq(hi, v);
            case DISTINCT_FROM -> false;
        };
    }

    private static boolean inExcludes(FilterExpression.In in, Map<String, ColumnStatistics> stats) {
        if (in.negated() || !(in.expression() instanceof FilterExpression.ColumnRef col)) return false;
        ColumnStatistics s = stats.get(col.columnName());
        if (s == null) return false;
        List<Object> values;
        ArrowType valueType;
        switch (in.set()) {
            case FilterExpression.LiteralSet set -> {
                values = set.values();
                valueType = set.field() == null ? null : set.field().getType();
            }
            case FilterExpression.ExternalSet set -> {
                values = set.values();
                valueType = set.field() == null ? null : set.field().getType();
            }
        }
        if (valueType != null && valueType instanceof ArrowType.List) valueType = null;
        if (valueType != null && !comparableTypes(col, valueType)) return false;
        List<Object> nonNull = values.stream().filter(Objects::nonNull).toList();
        if (nonNull.isEmpty()) return false;
        if (!s.hasNotNull()) return true;
        for (Object v : nonNull) {
            if (!outside(v, s.min(), s.max())) return false;
        }
        return true;
    }

    private static ComparisonOperator flip(ComparisonOperator op) {
        return switch (op) {
            case GT -> ComparisonOperator.LT;
            case GE -> ComparisonOperator.LE;
            case LT -> ComparisonOperator.GT;
            case LE -> ComparisonOperator.GE;
            default -> op;
        };
    }

    /** {@code v} provably lies outside {@code [lo, hi]} (a null bound is unbounded). */
    private static boolean outside(Object v, Object lo, Object hi) {
        return less(v, lo) || less(hi, v);
    }

    private static boolean less(Object a, Object b) {
        Integer c = compare(a, b);
        return c != null && c < 0;
    }

    private static boolean atMost(Object a, Object b) {
        Integer c = compare(a, b);
        return c != null && c <= 0;
    }

    private static boolean eq(Object a, Object b) {
        Integer c = compare(a, b);
        return c != null && c == 0;
    }

    /**
     * A literal is only compared to statistics when its type means the same
     * thing as the column's: any two numbers, any two strings, any two binaries,
     * otherwise identical Arrow types. A date literal against a timestamp column
     * (epoch days vs. microseconds) is not comparable.
     */
    private static boolean comparableTypes(FilterExpression.ColumnRef col, ArrowType literal) {
        if (col.field() == null || literal == null) return false;
        ArrowType column = col.field().getType();
        if (numeric(column) && numeric(literal)) return true;
        if (string(column) && string(literal)) return true;
        if (binary(column) && binary(literal)) return true;
        return column.equals(literal);
    }

    private static boolean numeric(ArrowType t) {
        return t instanceof ArrowType.Int || t instanceof ArrowType.FloatingPoint || t instanceof ArrowType.Decimal;
    }

    private static boolean string(ArrowType t) {
        return t instanceof ArrowType.Utf8 || t instanceof ArrowType.LargeUtf8 || t instanceof ArrowType.Utf8View;
    }

    private static boolean binary(ArrowType t) {
        return t instanceof ArrowType.Binary || t instanceof ArrowType.LargeBinary
                || t instanceof ArrowType.BinaryView;
    }

    /** Compare two scalar values, or {@code null} when they cannot be ordered against each other. */
    static Integer compare(Object a, Object b) {
        Object x = normalize(a);
        Object y = normalize(b);
        if (x == null || y == null) return null;
        if (x instanceof BigDecimal p && y instanceof BigDecimal q) return p.compareTo(q);
        if (x instanceof String p && y instanceof String q) return compareCodePoints(p, q);
        if (x instanceof byte[] p && y instanceof byte[] q) return Arrays.compareUnsigned(p, q);
        if (x instanceof Boolean p && y instanceof Boolean q) return p.compareTo(q);
        if (x.getClass() == y.getClass() && x instanceof Comparable<?>) {
            @SuppressWarnings("unchecked")
            Comparable<Object> p = (Comparable<Object>) x;
            return p.compareTo(y);
        }
        return null;
    }

    private static Object normalize(Object v) {
        return switch (v) {
            case null -> null;
            case java.time.LocalDate d -> BigDecimal.valueOf(d.toEpochDay());
            case Byte n -> BigDecimal.valueOf(n);
            case Short n -> BigDecimal.valueOf(n);
            case Integer n -> BigDecimal.valueOf(n);
            case Long n -> BigDecimal.valueOf(n);
            case BigInteger n -> new BigDecimal(n);
            case BigDecimal n -> n;
            case Float f -> Float.isFinite(f) ? new BigDecimal(f.toString()) : null;
            case Double d -> Double.isFinite(d) ? BigDecimal.valueOf(d) : null;
            case org.apache.arrow.vector.util.Text t -> t.toString();
            case CharSequence s -> s.toString();
            default -> v;
        };
    }

    /** UTF-8 byte order, which equals Unicode code point order (DuckDB's VARCHAR order). */
    private static int compareCodePoints(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int ca = a.codePointAt(i);
            int cb = b.codePointAt(j);
            if (ca != cb) return Integer.compare(ca, cb);
            i += Character.charCount(ca);
            j += Character.charCount(cb);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }
}
