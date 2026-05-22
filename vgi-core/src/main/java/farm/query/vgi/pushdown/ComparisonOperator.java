// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.pushdown;

/**
 * The six comparison operators a {@link PushdownFilter.Constant} can carry.
 * Wire tokens ({@code eq}, {@code ne}, ...) come from the C++ extension's
 * filter-spec JSON.
 */
public enum ComparisonOperator {
    EQ("eq", "="),
    NE("ne", "!="),
    GT("gt", ">"),
    GE("ge", ">="),
    LT("lt", "<"),
    LE("le", "<=");

    private final String wireToken;
    private final String symbol;

    ComparisonOperator(String wireToken, String symbol) {
        this.wireToken = wireToken;
        this.symbol = symbol;
    }

    public String wireToken() {
        return wireToken;
    }

    /** SQL-like symbol used by the diagnostic format helpers. */
    public String symbol() {
        return symbol;
    }

    public static ComparisonOperator fromWire(String token) {
        for (ComparisonOperator op : values()) {
            if (op.wireToken.equals(token)) return op;
        }
        throw new IllegalArgumentException("unknown comparison operator: '" + token + "'");
    }

    /** Apply this operator to a {@link Comparable#compareTo} result. */
    public boolean test(int cmp) {
        return switch (this) {
            case EQ -> cmp == 0;
            case NE -> cmp != 0;
            case GT -> cmp > 0;
            case GE -> cmp >= 0;
            case LT -> cmp < 0;
            case LE -> cmp <= 0;
        };
    }

    /**
     * Apply this operator when only equality is known (unordered operands).
     * Only {@code EQ}/{@code NE} yield a definite answer; ordered comparisons
     * against unordered operands are treated as non-matching.
     */
    public boolean testEquality(boolean equal) {
        return switch (this) {
            case EQ -> equal;
            case NE -> !equal;
            default -> false;
        };
    }
}
