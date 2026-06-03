// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

/** How null inputs propagate. Mirrors vgi-go {@code NullHandling}. */
public enum NullHandling {
    /** Skip null inputs / propagate nulls automatically. */
    DEFAULT,
    /** Pass nulls through; function decides. */
    SPECIAL
}
