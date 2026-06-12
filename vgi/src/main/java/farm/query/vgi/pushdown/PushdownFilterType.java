// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

/** Discriminator tag for a filter spec entry in the pushdown wire JSON. */
public enum PushdownFilterType {
    /** {@code column OP value} comparison. */
    CONSTANT("constant"),
    /** {@code column IS NULL}. */
    IS_NULL("is_null"),
    /** {@code column IS NOT NULL}. */
    IS_NOT_NULL("is_not_null"),
    /** {@code column IN (values...)}. */
    IN("in"),
    /** Membership against values supplied out-of-band in {@code InitRequest.join_keys}. */
    JOIN_KEYS("join_keys"),
    /** Conjunction of children. */
    AND("and"),
    /** Disjunction of children. */
    OR("or"),
    /** Filter that recurses into a struct field. */
    STRUCT("struct"),
    /** Recursive expression-tree predicate (e.g. {@code geom && box}), evaluated by the worker. */
    EXPRESSION("expression");

    private final String wireToken;

    PushdownFilterType(String wireToken) {
        this.wireToken = wireToken;
    }

    /**
     * The wire token for this filter type (e.g. {@code "constant"}).
     *
     * @return the pushdown filter-spec JSON {@code type} token
     */
    public String wireToken() {
        return wireToken;
    }

    /**
     * Resolve a wire token, or {@code null} if it isn't a recognised type.
     *
     * @param token the filter-spec JSON {@code type} token
     * @return the matching type, or {@code null} when unrecognised
     */
    public static PushdownFilterType fromWire(String token) {
        for (PushdownFilterType t : values()) {
            if (t.wireToken.equals(token)) return t;
        }
        return null;
    }
}
