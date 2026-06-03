// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

/**
 * Per-execution inputs handed to {@link TableInOutFunction#createExchange} when
 * a stream begins: the bound arguments, the negotiated input and (possibly
 * projection-narrowed) output schemas, and the allocator the exchange must use.
 *
 * @param functionName the bound function's name as invoked in SQL.
 * @param arguments the resolved positional/named arguments.
 * @param inputSchema the Arrow schema of the input stream.
 * @param outputSchema the output schema, already narrowed to the projected
 *     columns when the function opts into projection pushdown.
 * @param settings the session settings in effect for this execution.
 * @param allocator the allocator the exchange must use for all output vectors.
 */
public record TableInOutInitParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Schema outputSchema,
        Map<String, Object> settings,
        BufferAllocator allocator) {
}
