// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

/** Diagnostic origin of a v2 predicate. */
public enum PredicateSource {
    QUERY("query"), JOIN("join"), TOP_N("top_n"),
    SPLIT_REFINEMENT("split_refinement"), OTHER("other");

    private final String wireName;

    PredicateSource(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    static PredicateSource fromWire(String value) {
        for (var source : values()) if (source.wireName.equals(value)) return source;
        throw new FilterV2Exception("unknown predicate source " + value);
    }
}
