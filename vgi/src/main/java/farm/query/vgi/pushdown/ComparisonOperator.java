// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

/**
 * The six comparison operators a {@link PushdownFilter.Constant} can carry.
 * Wire tokens ({@code eq}, {@code ne}, ...) come from the C++ extension's
 * filter-spec JSON.
 */
public enum ComparisonOperator {
    /** Equal ({@code =}). */
    EQ("eq", "="),
    /** Not equal ({@code !=}). */
    NE("ne", "!="),
    /** Greater than ({@code >}). */
    GT("gt", ">"),
    /** Greater than or equal ({@code >=}). */
    GE("ge", ">="),
    /** Less than ({@code <}). */
    LT("lt", "<"),
    /** Less than or equal ({@code <=}). */
    LE("le", "<=");

    private final String wireToken;
    private final String symbol;

    ComparisonOperator(String wireToken, String symbol) {
        this.wireToken = wireToken;
        this.symbol = symbol;
    }

    /**
     * The wire token for this operator (e.g. {@code "eq"}).
     *
     * @return the C++ filter-spec JSON token
     */
    public String wireToken() {
        return wireToken;
    }

    /**
     * SQL-like symbol used by the diagnostic format helpers.
     *
     * @return the symbol (e.g. {@code "="}, {@code ">="})
     */
    public String symbol() {
        return symbol;
    }

    /**
     * Resolve a wire token to its operator.
     *
     * @param token the C++ filter-spec JSON token (e.g. {@code "eq"})
     * @return the matching operator
     * @throws IllegalArgumentException if {@code token} is not a known operator
     */
    public static ComparisonOperator fromWire(String token) {
        for (ComparisonOperator op : values()) {
            if (op.wireToken.equals(token)) return op;
        }
        throw new IllegalArgumentException("unknown comparison operator: '" + token + "'");
    }

    /**
     * Apply this operator to a {@link Comparable#compareTo} result.
     *
     * @param cmp the sign-significant comparison result ({@code <0}, {@code 0}, {@code >0})
     * @return whether the comparison satisfies this operator
     */
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
     *
     * @param equal whether the two operands are equal
     * @return whether this operator is satisfied given only the equality result
     */
    public boolean testEquality(boolean equal) {
        return switch (this) {
            case EQ -> equal;
            case NE -> !equal;
            default -> false;
        };
    }
}
