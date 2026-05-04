// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;

/** Parameters passed to {@link TableFunction#createProducer}. */
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
        Long tablesampleSeed) {

    /** Convenience ctor for callers that don't supply pushdown info. */
    public TableInitParams(String functionName, Arguments arguments, Schema outputSchema,
                            Map<String, Object> settings, BufferAllocator allocator) {
        this(functionName, arguments, outputSchema, settings, allocator,
                null, List.of(), List.of(), null, null);
    }

    /** Convenience ctor without tablesample hints. */
    public TableInitParams(String functionName, Arguments arguments, Schema outputSchema,
                            Map<String, Object> settings, BufferAllocator allocator,
                            byte[] pushdownFilters, List<Integer> projectionIds,
                            List<byte[]> joinKeys) {
        this(functionName, arguments, outputSchema, settings, allocator,
                pushdownFilters, projectionIds, joinKeys, null, null);
    }
}
