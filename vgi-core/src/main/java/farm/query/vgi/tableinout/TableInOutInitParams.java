// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.tableinout;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

public record TableInOutInitParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Schema outputSchema,
        Map<String, Object> settings,
        BufferAllocator allocator) {
}
