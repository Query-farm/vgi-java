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
        OrderPreservation orderPreservation,
        boolean supportsBatchIndex,
        PartitionKind partitionKind) {

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

    /**
     * Wire enum for {@code FunctionInfo.partition_kind} — the partition shape
     * a table function declares over its {@code vgi.partition_column}-annotated
     * output-schema fields. Mirrors vgi-python's {@code PartitionKind}. DuckDB
     * consumes only {@link #SINGLE_VALUE_PARTITIONS} today (plans
     * {@code PhysicalPartitionedAggregate}); the others are wire-declarable and
     * fall back to {@code HASH_GROUP_BY}.
     */
    public enum PartitionKind {
        NOT_PARTITIONED, SINGLE_VALUE_PARTITIONS, OVERLAPPING_PARTITIONS, DISJOINT_PARTITIONS
    }

    public FunctionMetadata(String description, Stability stability, NullHandling nullHandling,
                              boolean autoApplyFilters, boolean projectionPushdown,
                              boolean filterPushdown, boolean samplingPushdown,
                              List<String> categories, OrderPreservation orderPreservation) {
        this(description, stability, nullHandling, autoApplyFilters, projectionPushdown,
                filterPushdown, samplingPushdown, categories, orderPreservation,
                false, PartitionKind.NOT_PARTITIONED);
    }

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
                samplingPushdown, categories, orderPreservation, supportsBatchIndex, partitionKind);
    }

    public FunctionMetadata withSamplingPushdown() {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, true, categories, orderPreservation,
                supportsBatchIndex, partitionKind);
    }

    public FunctionMetadata withCategories(String... cats) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, List.of(cats), orderPreservation,
                supportsBatchIndex, partitionKind);
    }

    public FunctionMetadata withOrderPreservation(OrderPreservation op) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, op,
                supportsBatchIndex, partitionKind);
    }

    /** Opt into {@code supports_batch_index}: every emitted batch must carry a
     *  {@code vgi_batch_index} tag (see {@code EmitMetadata#batchIndex}). */
    public FunctionMetadata withBatchIndex() {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, orderPreservation,
                true, partitionKind);
    }

    /** Declare a non-default {@link PartitionKind} over the output schema's
     *  {@code vgi.partition_column}-annotated fields. */
    public FunctionMetadata withPartitionKind(PartitionKind kind) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, orderPreservation,
                supportsBatchIndex, kind);
    }
}
