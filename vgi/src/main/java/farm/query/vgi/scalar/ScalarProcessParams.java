// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

/**
 * Parameters passed to {@link ScalarFunction#process}.
 *
 * @param functionName SQL name the binding was invoked under.
 * @param arguments resolved const positional / named arguments.
 * @param outputSchema schema the produced output batch must match (from {@link ScalarFunction#onBind}).
 * @param settings DuckDB session settings declared by the worker.
 * @param secrets opaque secrets payload for the invocation, or {@code null}.
 */
public record ScalarProcessParams(
        String functionName,
        Arguments arguments,
        Schema outputSchema,
        Map<String, Object> settings,
        byte[] secrets) {
}
