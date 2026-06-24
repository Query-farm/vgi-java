// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

import farm.query.vgi.protocol.FunctionExample;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata describing a VGI function. Mirrors vgi-go {@code FunctionMetadata}.
 *
 * @param description         human-readable function description.
 * @param stability           output determinism ({@link Stability}).
 * @param nullHandling        how null inputs propagate ({@link NullHandling}).
 * @param autoApplyFilters    whether the engine may auto-apply pushed-down filters.
 * @param projectionPushdown  whether the function accepts projection pushdown.
 * @param filterPushdown      whether the function accepts filter pushdown.
 * @param samplingPushdown    whether the function accepts sampling pushdown.
 * @param categories          SQL function categories for discovery.
 * @param orderPreservation   declared output ordering guarantee ({@link OrderPreservation}).
 * @param supportsBatchIndex  whether emitted batches carry a {@code vgi_batch_index} tag.
 * @param partitionKind       partition shape over partition-column-annotated output fields.
 * @param lateMaterialization whether the function opts into DuckDB late materialization.
 * @param supportedExpressionFilters expression-filter function names this function can apply itself.
 * @param examples            documented usage examples surfaced on {@code FunctionInfo.examples}.
 * @param tags                arbitrary worker-provided metadata tags surfaced on
 *                            {@code FunctionInfo.tags} (e.g. {@code vgi.columns_md}).
 */
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
        PartitionKind partitionKind,
        boolean lateMaterialization,
        List<String> supportedExpressionFilters,
        List<FunctionExample> examples,
        Map<String, String> tags) {

    /**
     * Compact constructor: normalise {@code tags} to a non-null, defensively
     * copied map so {@link #tags()} never returns {@code null} and callers can't
     * mutate the metadata's tags through the original reference.
     */
    public FunctionMetadata {
        tags = tags == null ? Map.of() : Map.copyOf(tags);
    }

    /**
     * Wire enum for {@code FunctionInfo.order_preservation}. Mirrors the
     * three values DuckDB recognises in
     * {@code TableFunction::order_preservation_type}.
     */
    public enum OrderPreservation {
        /** Planner is free to parallelise and reorder; output ordering is undefined. */
        NO_ORDER_PRESERVED("NO_ORDER_GUARANTEE"),
        /** Insertion / production order is preserved within each parallel
         *  output stream; the planner can still parallelise. */
        INSERTION_ORDER("PRESERVES_ORDER"),
        /** The planner serialises the entire pipeline onto a single worker so
         *  the function's output is observed in exact emission order. */
        FIXED_ORDER("FIXED_ORDER");

        private final String wireName;

        OrderPreservation(String wireName) { this.wireName = wireName; }

        /** The canonical on-wire {@code order_preservation} string (mirrors
         *  vgi-python's {@code OrderPreservation} enum names, which the C++
         *  parser validates against). The Java constant names follow DuckDB's
         *  {@code OrderPreservationType} instead, so they differ.
         *
         *  @return the wire string for this value */
        public String wireName() { return wireName; }
    }

    /**
     * Wire enum for {@code FunctionInfo.partition_kind} — the partition shape
     * a table function declares over its {@code vgi.partition_column}-annotated
     * output-schema fields. Mirrors vgi-python's {@code PartitionKind}. DuckDB
     * consumes only {@link #SINGLE_VALUE_PARTITIONS} today (plans
     * {@code PhysicalPartitionedAggregate}); the others are wire-declarable and
     * fall back to {@code HASH_GROUP_BY}.
     */
    public enum PartitionKind {
        /** No partitioning declared over the annotated columns (default; same
         *  effect as leaving fields un-annotated). */
        NOT_PARTITIONED,
        /** Each emitted batch carries exactly one distinct value per partition
         *  column. Unlocks {@code PhysicalPartitionedAggregate} for
         *  {@code GROUP BY} over those columns. */
        SINGLE_VALUE_PARTITIONS,
        /** Per-batch value ranges overlap only at boundaries (bounds like
         *  {@code [1,2][2,3][3,4]}). Wire-level declarable; DuckDB has no
         *  consumer today. */
        OVERLAPPING_PARTITIONS,
        /** Per-batch value ranges are pairwise disjoint (bounds like
         *  {@code [1,2][3,4][5,6]}). Wire-level declarable; DuckDB has no
         *  consumer today. */
        DISJOINT_PARTITIONS
    }

    /**
     * Convenience constructor defaulting batch-index, partition-kind, and late-materialization off.
     *
     * @param description        human-readable function description.
     * @param stability          output determinism ({@link Stability}).
     * @param nullHandling       how null inputs propagate ({@link NullHandling}).
     * @param autoApplyFilters   whether the engine may auto-apply pushed-down filters.
     * @param projectionPushdown whether the function accepts projection pushdown.
     * @param filterPushdown     whether the function accepts filter pushdown.
     * @param samplingPushdown   whether the function accepts sampling pushdown.
     * @param categories         SQL function categories for discovery.
     * @param orderPreservation  declared output ordering guarantee ({@link OrderPreservation}).
     */
    public FunctionMetadata(String description, Stability stability, NullHandling nullHandling,
                              boolean autoApplyFilters, boolean projectionPushdown,
                              boolean filterPushdown, boolean samplingPushdown,
                              List<String> categories, OrderPreservation orderPreservation) {
        this(description, stability, nullHandling, autoApplyFilters, projectionPushdown,
                filterPushdown, samplingPushdown, categories, orderPreservation,
                false, PartitionKind.NOT_PARTITIONED, false, List.of(), List.of(), Map.of());
    }

    /**
     * Convenience constructor with no declared order preservation.
     *
     * @param description        human-readable function description.
     * @param stability          output determinism ({@link Stability}).
     * @param nullHandling       how null inputs propagate ({@link NullHandling}).
     * @param autoApplyFilters   whether the engine may auto-apply pushed-down filters.
     * @param projectionPushdown whether the function accepts projection pushdown.
     * @param filterPushdown     whether the function accepts filter pushdown.
     * @param samplingPushdown   whether the function accepts sampling pushdown.
     * @param categories         SQL function categories for discovery.
     */
    public FunctionMetadata(String description, Stability stability, NullHandling nullHandling,
                              boolean autoApplyFilters, boolean projectionPushdown,
                              boolean filterPushdown, boolean samplingPushdown,
                              List<String> categories) {
        this(description, stability, nullHandling, autoApplyFilters, projectionPushdown,
                filterPushdown, samplingPushdown, categories, null);
    }

    /**
     * Convenience constructor with no categories and no declared order preservation.
     *
     * @param description        human-readable function description.
     * @param stability          output determinism ({@link Stability}).
     * @param nullHandling       how null inputs propagate ({@link NullHandling}).
     * @param autoApplyFilters   whether the engine may auto-apply pushed-down filters.
     * @param projectionPushdown whether the function accepts projection pushdown.
     * @param filterPushdown     whether the function accepts filter pushdown.
     * @param samplingPushdown   whether the function accepts sampling pushdown.
     */
    public FunctionMetadata(String description, Stability stability, NullHandling nullHandling,
                              boolean autoApplyFilters, boolean projectionPushdown,
                              boolean filterPushdown, boolean samplingPushdown) {
        this(description, stability, nullHandling, autoApplyFilters, projectionPushdown,
                filterPushdown, samplingPushdown, List.of(), null);
    }

    /**
     * Minimal metadata: description only, consistent/default with no pushdown.
     *
     * @param description the function description.
     * @return a baseline {@link FunctionMetadata}.
     */
    public static FunctionMetadata describe(String description) {
        return new FunctionMetadata(description, Stability.CONSISTENT, NullHandling.DEFAULT, false, false, false, false);
    }

    /**
     * Builder convenience: same description, opt into filter+projection pushdown.
     *
     * @param projection whether to accept projection pushdown.
     * @param filter     whether to accept filter pushdown.
     * @param autoApply  whether the engine may auto-apply pushed-down filters.
     * @return a copy with the pushdown flags set.
     */
    public FunctionMetadata withPushdown(boolean projection, boolean filter, boolean autoApply) {
        return new FunctionMetadata(description, stability, nullHandling, autoApply, projection, filter,
                samplingPushdown, categories, orderPreservation, supportsBatchIndex, partitionKind,
                lateMaterialization, supportedExpressionFilters, examples, tags);
    }

    /**
     * Opt into sampling pushdown.
     *
     * @return a copy with {@code samplingPushdown} enabled.
     */
    public FunctionMetadata withSamplingPushdown() {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, true, categories, orderPreservation,
                supportsBatchIndex, partitionKind, lateMaterialization, supportedExpressionFilters,
                examples, tags);
    }

    /**
     * Set the SQL function categories.
     *
     * @param cats category names.
     * @return a copy with the given categories.
     */
    public FunctionMetadata withCategories(String... cats) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, List.of(cats), orderPreservation,
                supportsBatchIndex, partitionKind, lateMaterialization, supportedExpressionFilters,
                examples, tags);
    }

    /**
     * Set the declared output ordering guarantee.
     *
     * @param op the {@link OrderPreservation} value.
     * @return a copy with the given order preservation.
     */
    public FunctionMetadata withOrderPreservation(OrderPreservation op) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, op,
                supportsBatchIndex, partitionKind, lateMaterialization, supportedExpressionFilters,
                examples, tags);
    }

    /** Opt into {@code supports_batch_index}: every emitted batch must carry a
     *  {@code vgi_batch_index} tag (see {@code EmitMetadata#batchIndex}).
     *
     *  @return a copy with batch-index support enabled. */
    public FunctionMetadata withBatchIndex() {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, orderPreservation,
                true, partitionKind, lateMaterialization, supportedExpressionFilters, examples, tags);
    }

    /** Declare a non-default {@link PartitionKind} over the output schema's
     *  {@code vgi.partition_column}-annotated fields.
     *
     *  @param kind the partition shape to declare.
     *  @return a copy with the given partition kind. */
    public FunctionMetadata withPartitionKind(PartitionKind kind) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, orderPreservation,
                supportsBatchIndex, kind, lateMaterialization, supportedExpressionFilters, examples, tags);
    }

    /** Opt into DuckDB's late-materialization optimizer. Only meaningful for a
     *  table function whose output exposes an {@code is_row_id} virtual column
     *  and that also declares filter + projection pushdown (see
     *  {@code late_materialization.test}).
     *
     *  @return a copy with late materialization enabled. */
    public FunctionMetadata withLateMaterialization() {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, orderPreservation,
                supportsBatchIndex, partitionKind, true, supportedExpressionFilters, examples, tags);
    }

    /** Declare the expression-filter function names this table function can
     *  receive pushed down and apply itself (e.g. {@code "&&"},
     *  {@code "st_intersects_extent"}, {@code "list_contains"}). The engine only
     *  pushes an expression filter into the function when every function name in
     *  the predicate tree appears here; otherwise it keeps a FILTER node above
     *  the scan. Surfaced on the wire as {@code FunctionInfo.supported_expression_filters}.
     *
     *  @param names the supported expression-filter function names.
     *  @return a copy declaring the given supported expression filters. */
    public FunctionMetadata withSupportedExpressionFilters(String... names) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, orderPreservation,
                supportsBatchIndex, partitionKind, lateMaterialization, List.of(names), examples, tags);
    }

    /** Declare the documented usage examples surfaced on {@code FunctionInfo.examples}.
     *  Each {@link FunctionExample} carries an example {@code sql} string, a
     *  human-readable {@code description}, and an optional {@code expected_output}.
     *
     *  @param examples the usage examples to advertise.
     *  @return a copy carrying the given examples. */
    public FunctionMetadata withExamples(List<FunctionExample> examples) {
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, orderPreservation,
                supportsBatchIndex, partitionKind, lateMaterialization, supportedExpressionFilters,
                examples == null ? List.of() : examples, tags);
    }

    /** Merge worker-provided metadata tags surfaced on {@code FunctionInfo.tags}
     *  and reported through DuckDB's function {@code tags} map. Typical keys are
     *  {@code vgi.columns_md} (required by lint rule VGI307 for table functions
     *  with a dynamic schema) and {@code vgi.description_md}. The given tags are
     *  merged into any already declared on this metadata (later keys overwrite
     *  duplicates); existing tags not present in {@code more} are preserved. Any
     *  tags the SDK derives internally and any prior worker tags are kept — this
     *  never clobbers them wholesale.
     *
     *  @param more the tag key/value pairs to merge in.
     *  @return a copy carrying the merged tags. */
    public FunctionMetadata withTags(Map<String, String> more) {
        Map<String, String> merged = new LinkedHashMap<>(tags);
        if (more != null) merged.putAll(more);
        return new FunctionMetadata(description, stability, nullHandling, autoApplyFilters,
                projectionPushdown, filterPushdown, samplingPushdown, categories, orderPreservation,
                supportsBatchIndex, partitionKind, lateMaterialization, supportedExpressionFilters,
                examples, merged);
    }

    /** Add or overwrite a single metadata tag (e.g. {@code vgi.columns_md}),
     *  merging with any tags already declared. See {@link #withTags(Map)}.
     *
     *  @param key   the tag key.
     *  @param value the tag value.
     *  @return a copy carrying the added tag. */
    public FunctionMetadata withTag(String key, String value) {
        return withTags(Map.of(key, value));
    }
}
