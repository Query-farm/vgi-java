// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

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

    /** All-fields excl. secrets + attachId — compat ctor for older call sites. */
    public TableInitParams(String functionName, Arguments arguments, Schema outputSchema,
                            Map<String, Object> settings, BufferAllocator allocator,
                            byte[] pushdownFilters, List<Integer> projectionIds,
                            List<byte[]> joinKeys, Double tablesamplePercentage,
                            Long tablesampleSeed, String orderByColumnName,
                            String orderByDirection, String orderByNullOrder,
                            Long orderByLimit, byte[] executionId) {
        this(functionName, arguments, outputSchema, settings, allocator,
                pushdownFilters, projectionIds, joinKeys, tablesamplePercentage,
                tablesampleSeed, orderByColumnName, orderByDirection,
                orderByNullOrder, orderByLimit, executionId, null, null);
    }

    /** All-fields excl. attachId — compat ctor for older call sites. */
    public TableInitParams(String functionName, Arguments arguments, Schema outputSchema,
                            Map<String, Object> settings, BufferAllocator allocator,
                            byte[] pushdownFilters, List<Integer> projectionIds,
                            List<byte[]> joinKeys, Double tablesamplePercentage,
                            Long tablesampleSeed, String orderByColumnName,
                            String orderByDirection, String orderByNullOrder,
                            Long orderByLimit, byte[] executionId, byte[] secrets) {
        this(functionName, arguments, outputSchema, settings, allocator,
                pushdownFilters, projectionIds, joinKeys, tablesamplePercentage,
                tablesampleSeed, orderByColumnName, orderByDirection,
                orderByNullOrder, orderByLimit, executionId, secrets, null);
    }

    /** Convenience ctor for callers that don't supply pushdown info. */
    public TableInitParams(String functionName, Arguments arguments, Schema outputSchema,
                            Map<String, Object> settings, BufferAllocator allocator) {
        this(functionName, arguments, outputSchema, settings, allocator,
                null, List.of(), List.of(), null, null, null, null, null, null, null);
    }

    /** Convenience ctor without tablesample hints. */
    public TableInitParams(String functionName, Arguments arguments, Schema outputSchema,
                            Map<String, Object> settings, BufferAllocator allocator,
                            byte[] pushdownFilters, List<Integer> projectionIds,
                            List<byte[]> joinKeys) {
        this(functionName, arguments, outputSchema, settings, allocator,
                pushdownFilters, projectionIds, joinKeys, null, null, null, null, null, null, null);
    }

    /** Convenience ctor without executionId — kept for source-compat with callers
     * that predate the addition of the field. */
    public TableInitParams(String functionName, Arguments arguments, Schema outputSchema,
                            Map<String, Object> settings, BufferAllocator allocator,
                            byte[] pushdownFilters, List<Integer> projectionIds,
                            List<byte[]> joinKeys, Double tablesamplePercentage,
                            Long tablesampleSeed, String orderByColumnName,
                            String orderByDirection, String orderByNullOrder,
                            Long orderByLimit) {
        this(functionName, arguments, outputSchema, settings, allocator,
                pushdownFilters, projectionIds, joinKeys, tablesamplePercentage,
                tablesampleSeed, orderByColumnName, orderByDirection,
                orderByNullOrder, orderByLimit, null);
    }
}
