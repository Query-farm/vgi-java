// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

/** Function output determinism. Mirrors vgi-go {@code FunctionStability}. */
public enum Stability {
    /** Always returns the same output for the same input. */
    CONSISTENT,
    /** Stable within a single query, may differ across queries. */
    CONSISTENT_WITHIN_QUERY,
    /** May return a different result on every invocation. */
    VOLATILE
}
