// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.pushdown;

/** Discriminator tag for a filter spec entry in the pushdown wire JSON. */
public enum PushdownFilterType {
    CONSTANT("constant"),
    IS_NULL("is_null"),
    IS_NOT_NULL("is_not_null"),
    IN("in"),
    JOIN_KEYS("join_keys"),
    AND("and"),
    OR("or"),
    STRUCT("struct");

    private final String wireToken;

    PushdownFilterType(String wireToken) {
        this.wireToken = wireToken;
    }

    public String wireToken() {
        return wireToken;
    }

    /** Resolve a wire token, or {@code null} if it isn't a recognised type. */
    public static PushdownFilterType fromWire(String token) {
        for (PushdownFilterType t : values()) {
            if (t.wireToken.equals(token)) return t;
        }
        return null;
    }
}
