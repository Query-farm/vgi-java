// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

/** Monotonicity of a scalar function in one argument, with all others fixed. */
public enum ArgumentMonotonicity {
    UNKNOWN,
    CONSTANT,
    NON_DECREASING,
    STRICTLY_INCREASING,
    NON_INCREASING,
    STRICTLY_DECREASING;

    /** @return the canonical uppercase VGI wire value. */
    public String wireName() { return name(); }
}
