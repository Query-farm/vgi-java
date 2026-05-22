// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.tableinout;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

public record TableInOutBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings) {
}
