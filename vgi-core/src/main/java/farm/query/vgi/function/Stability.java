// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.function;

/** Function output determinism. Mirrors vgi-go {@code FunctionStability}. */
public enum Stability {
    CONSISTENT,
    CONSISTENT_WITHIN_QUERY,
    VOLATILE
}
