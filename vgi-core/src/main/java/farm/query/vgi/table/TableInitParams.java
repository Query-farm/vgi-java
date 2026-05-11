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
}
