// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

/** Immutable evaluation-context metadata attached to a filter batch. */
public record FilterEvaluationContext(
        String profile,
        String timeZone,
        String calendar,
        String defaultCollation,
        Boolean ieeeFloatingPointOps,
        Boolean integerDivision,
        String providerFingerprint) {

    public static FilterEvaluationContext none() {
        return new FilterEvaluationContext("vgi.none.v1", null, null, null, null, null, null);
    }
}
