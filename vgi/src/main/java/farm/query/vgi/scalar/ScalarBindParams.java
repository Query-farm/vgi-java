// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

/**
 * Parameters passed to {@link ScalarFunction#onBind}.
 *
 * <p>{@code inputSchema} may be {@code null} if no input columns were bound.
 * {@code settings} carries DuckDB session settings the worker declared.
 *
 * @param functionName SQL name the binding was requested under.
 * @param arguments resolved const positional / named arguments.
 * @param inputSchema schema of the bound input columns, or {@code null} when none.
 * @param settings DuckDB session settings declared by the worker.
 * @param secrets opaque secrets payload for the binding, or {@code null}.
 * @param resolvedSecretsProvided whether {@code secrets} carries resolved values.
 */
public record ScalarBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings,
        byte[] secrets,
        boolean resolvedSecretsProvided) {

    /**
     * Convenience constructor for bindings with no secrets.
     *
     * @param functionName SQL name the binding was requested under.
     * @param arguments resolved const positional / named arguments.
     * @param inputSchema schema of the bound input columns, or {@code null} when none.
     * @param settings DuckDB session settings declared by the worker.
     */
    public ScalarBindParams(String functionName, Arguments arguments, Schema inputSchema,
                              Map<String, Object> settings) {
        this(functionName, arguments, inputSchema, settings, null, false);
    }
}
