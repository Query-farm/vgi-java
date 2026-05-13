// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.pushdown.FilterApplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parameters passed to {@link TableFunction#createProducer}.
 *
 * <p>{@code executionId} is the per-execution identifier DuckDB threads
 * through every callback (init, statistics, dynamic_to_string). Producers
 * that publish per-execution diagnostics keep state keyed off this byte[]
 * so the {@link TableFunction#dynamicToString} hook can match the snapshot
 * back to its scan.
 */
public record TableInitParams(
        String functionName,
        Arguments arguments,
        Schema outputSchema,
        Map<String, Object> settings,
        BufferAllocator allocator,
        byte[] pushdownFilters,
        List<Integer> projectionIds,
        List<byte[]> joinKeys,
        Double tablesamplePercentage,
        Long tablesampleSeed,
        String orderByColumnName,
        String orderByDirection,
        String orderByNullOrder,
        Long orderByLimit,
        byte[] executionId,
        byte[] secrets,
        byte[] attachId) {

    /**
     * Wrap the raw {@link #pushdownFilters} / {@link #joinKeys} bytes into a
     * {@link FilterApplier} ready to call {@code apply(root)} on each emitted
     * batch. Cheap to construct — the actual decode happens lazily on first
     * {@code apply}. Producers should cache the returned applier on their
     * state (see {@link TableProducerState}) so the decode runs once.
     */
    public FilterApplier filters() {
        return FilterApplier.from(pushdownFilters, joinKeys);
    }

    /**
     * Return the subset of {@link #outputSchema} that DuckDB actually asked
     * for via {@link #projectionIds}. When no projection-pushdown is in play
     * (null/empty {@code projectionIds}), returns {@code outputSchema}
     * unchanged. Use this to allocate the destination root when honouring
     * projection pushdown.
     */
    public Schema projectedOutputSchema() {
        if (outputSchema == null) return null;
        if (projectionIds == null || projectionIds.isEmpty()) return outputSchema;
        List<Field> fields = outputSchema.getFields();
        List<Field> picked = new ArrayList<>(projectionIds.size());
        for (Integer i : projectionIds) {
            if (i != null && i >= 0 && i < fields.size()) picked.add(fields.get(i));
        }
        return new Schema(picked, outputSchema.getCustomMetadata());
    }
}
