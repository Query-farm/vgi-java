// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.function;

/** Predicate enum for "any"-typed argument validation. Mirrors vgi-go. */
public enum TypeBoundPredicate {
    IS_ADDABLE("numeric");

    private final String description;

    TypeBoundPredicate(String description) { this.description = description; }

    /** User-facing description ("numeric", "comparable", etc.). */
    public String description() { return description; }
}
