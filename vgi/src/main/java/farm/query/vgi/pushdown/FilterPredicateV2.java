// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

/** One independently revisioned predicate in a v2 snapshot or delta. */
public record FilterPredicateV2(
        String id,
        long revision,
        PredicateMode mode,
        PredicateSource source,
        FilterExpression expression) {}
