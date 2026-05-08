// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.pushdown;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Container for the parsed top-level filter list (implicit AND). */
public record PushdownFilters(List<PushdownFilter> filters, String version) {

    public static PushdownFilters empty() {
        return new PushdownFilters(List.of(), "1");
    }

    /**
     * Format the filters as a human-readable {@code AND}-joined SQL-like
     * string with values inlined. Used by diagnostic fixtures like
     * {@code filter_echo}; matches vgi-go's {@code formatFiltersInline}.
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
                    + symbol(c.op()) + " " + sqlLiteral(c.value()) + ")";
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
        };
    }

    private static String formatOne(PushdownFilter f) {
        return switch (f) {
            case PushdownFilter.Constant c -> c.columnName() + " " + symbol(c.op()) + " " + sqlLiteral(c.value());
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
        };
    }

    private static String symbol(String op) {
        return switch (op) {
            case "eq" -> "=";
            case "ne" -> "!=";
            case "gt" -> ">";
            case "ge" -> ">=";
            case "lt" -> "<";
            case "le" -> "<=";
            default -> op;
        };
    }

    private static String sqlLiteral(Object v) {
        if (v == null) return "NULL";
        if (v instanceof CharSequence s) return "'" + s + "'";
        if (v instanceof byte[] b) return "'" + new String(b, java.nio.charset.StandardCharsets.UTF_8) + "'";
        return v.toString();
    }

    /**
     * Evaluate every filter against {@code root} and return a boolean mask
     * (one entry per row) — {@code true} means the row passes all filters
     * (top-level AND). The mask is a {@code boolean[]} so callers can drive
     * fixture-side row filtering without an Arrow allocation.
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
                for (Object x : in.values()) if (compare("eq", cell, x)) yield true;
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
                for (Object x : in.values()) if (compare("eq", cell, x)) yield true;
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

    private static boolean compare(String op, Object a, Object b) {
        if (a == null || b == null) return false;
        // Normalise byte[] (Arrow VarBinary/VarChar) to string for comparison.
        if (a instanceof byte[] ab) a = new String(ab, java.nio.charset.StandardCharsets.UTF_8);
        if (b instanceof byte[] bb) b = new String(bb, java.nio.charset.StandardCharsets.UTF_8);
        // Arrow's getObject for VarChar returns Text; coerce both sides to String.
        a = a instanceof org.apache.arrow.vector.util.Text t ? t.toString() : a;
        b = b instanceof org.apache.arrow.vector.util.Text t ? t.toString() : b;
        if (a instanceof Number na && b instanceof Number nb) {
            int cmp = Double.compare(na.doubleValue(), nb.doubleValue());
            return applyCmp(op, cmp);
        }
        if (a instanceof Comparable && a.getClass() == b.getClass()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            int cmp = ((Comparable) a).compareTo(b);
            return applyCmp(op, cmp);
        }
        return switch (op) {
            case "eq" -> Objects.equals(a, b);
            case "ne" -> !Objects.equals(a, b);
            default -> false;
        };
    }

    private static boolean applyCmp(String op, int cmp) {
        return switch (op) {
            case "eq" -> cmp == 0;
            case "ne" -> cmp != 0;
            case "gt" -> cmp > 0;
            case "ge" -> cmp >= 0;
            case "lt" -> cmp < 0;
            case "le" -> cmp <= 0;
            default -> false;
        };
    }
}
