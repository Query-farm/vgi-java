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
        List<String> categories) {

    public FunctionMetadata(String description, Stability stability, NullHandling nullHandling,
                              boolean autoApplyFilters, boolean projectionPushdown,
                              boolean filterPushdown, boolean samplingPushdown) {
        this(description, stability, nullHandling, autoApplyFilters, projectionPushdown,
                filterPushdown, samplingPushdown, List.of());
    }

    public static FunctionMetadata defaults() {
        return new FunctionMetadata("", Stability.CONSISTENT, NullHandling.DEFAULT, false, false, false, false);
    }

    public static FunctionMetadata describe(String description) {
        return new FunctionMetadata(description, Stability.CONSISTENT, NullHandling.DEFAULT, false, false, false, false);
    }

    /** Builder convenience: same description, opt into filter+projection pushdown. */
    public FunctionMetadata withPushdown(boolean projection, boolean filter, boolean autoApply) {
        return new FunctionMetadata(description, stability, nullHandling, autoApply, projection, filter,
                samplingPushdown, categories);
    }

    public FunctionMetadata withSamplingPushdown() {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, true, categories);
    }

    public FunctionMetadata withCategories(String... cats) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, List.of(cats));
    }
}
