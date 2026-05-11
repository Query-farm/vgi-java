// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.function;

import java.util.List;

/** Metadata describing a VGI function. Mirrors vgi-go {@code FunctionMetadata}. */
public record FunctionMetadata(
        String description,
        Stability stability,
        NullHandling nullHandling,
        boolean autoApplyFilters,
        boolean projectionPushdown,
        boolean filterPushdown,
        boolean samplingPushdown,
        List<String> categories,
        OrderPreservation orderPreservation) {

    /**
     * Wire enum for {@code FunctionInfo.order_preservation}. Mirrors the
     * three values DuckDB recognises in {@code TableFunction::order_preservation_type}:
     *
     * <ul>
     *   <li>{@link #NO_ORDER_PRESERVED} — planner is free to parallelise
     *       and reorder; output ordering is undefined.</li>
     *   <li>{@link #INSERTION_ORDER} — insertion / production order is
     *       preserved within each parallel output stream; the planner can
     *       still parallelise.</li>
     *   <li>{@link #FIXED_ORDER} — the planner serialises the entire
     *       pipeline onto a single worker so the function's output is
     *       observed in exact emission order.</li>
     * </ul>
     */
    public enum OrderPreservation { NO_ORDER_PRESERVED, INSERTION_ORDER, FIXED_ORDER }

    public FunctionMetadata(String description, Stability stability, NullHandling nullHandling,
                              boolean autoApplyFilters, boolean projectionPushdown,
                              boolean filterPushdown, boolean samplingPushdown,
                              List<String> categories) {
        this(description, stability, nullHandling, autoApplyFilters, projectionPushdown,
                filterPushdown, samplingPushdown, categories, null);
    }

    public FunctionMetadata(String description, Stability stability, NullHandling nullHandling,
                              boolean autoApplyFilters, boolean projectionPushdown,
                              boolean filterPushdown, boolean samplingPushdown) {
        this(description, stability, nullHandling, autoApplyFilters, projectionPushdown,
                filterPushdown, samplingPushdown, List.of(), null);
    }

    public static FunctionMetadata describe(String description) {
        return new FunctionMetadata(description, Stability.CONSISTENT, NullHandling.DEFAULT, false, false, false, false);
    }

    /** Builder convenience: same description, opt into filter+projection pushdown. */
    public FunctionMetadata withPushdown(boolean projection, boolean filter, boolean autoApply) {
        return new FunctionMetadata(description, stability, nullHandling, autoApply, projection, filter,
                samplingPushdown, categories, orderPreservation);
    }

    public FunctionMetadata withSamplingPushdown() {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, true, categories, orderPreservation);
    }

    public FunctionMetadata withCategories(String... cats) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, List.of(cats), orderPreservation);
    }

    public FunctionMetadata withOrderPreservation(OrderPreservation op) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, op);
    }
}
