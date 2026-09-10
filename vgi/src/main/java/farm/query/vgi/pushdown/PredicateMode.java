// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

/** Whether a predicate is exact query semantics or an optional pruning hint. */
public enum PredicateMode {
    REQUIRED("required"),
    ADVISORY("advisory");

    private final String wireName;

    PredicateMode(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    static PredicateMode fromWire(String value) {
        for (var mode : values()) if (mode.wireName.equals(value)) return mode;
        throw new FilterV2Exception("unknown predicate mode " + value);
    }
}
