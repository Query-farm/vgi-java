// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.scalar;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

/**
 * Parameters passed to {@link ScalarFunction#onBind}.
 *
 * <p>{@code inputSchema} may be {@code null} if no input columns were bound.
 * {@code settings} carries DuckDB session settings the worker declared.
 */
public record ScalarBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings,
        byte[] secrets,
        boolean resolvedSecretsProvided) {

    public ScalarBindParams(String functionName, Arguments arguments, Schema inputSchema,
                              Map<String, Object> settings) {
        this(functionName, arguments, inputSchema, settings, null, false);
    }
}
