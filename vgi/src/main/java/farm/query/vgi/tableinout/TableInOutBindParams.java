// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

/**
 * Bind-time inputs for a {@link TableInOutFunction}: the resolved arguments and
 * the schema of the incoming stream, from which the function derives its output
 * schema.
 *
 * @param functionName the bound function's name as invoked in SQL.
 * @param arguments the resolved positional/named arguments.
 * @param inputSchema the Arrow schema of the input stream (may be {@code null}
 *     or empty during catalog enumeration).
 * @param settings the session settings in effect for this bind.
 */
public record TableInOutBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings) {
}
