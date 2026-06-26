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
 * @param secrets the resolved secret bytes, or {@code null} when none were sent;
 *     populated on the second bind pass of the two-phase secret bind.
 * @param resolvedSecretsProvided {@code true} on the re-bind that follows a
 *     first-pass secret-lookup request (see {@code BindResponse}); a function
 *     requests secrets on the first pass and reads them once this is true.
 * @param attachOpaqueData the catalog attach's opaque identifier bytes, or
 *     {@code null} during catalog enumeration.
 * @param attachStorage a storage facade scoped to {@code attachOpaqueData} —
 *     state that persists across queries within one ATTACH session;
 *     {@code null} during catalog enumeration.
 */
public record TableInOutBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings,
        byte[] secrets,
        boolean resolvedSecretsProvided,
        byte[] attachOpaqueData,
        farm.query.vgi.storage.BoundStorage attachStorage) {

    /**
     * Convenience constructor with no attach context (catalog enumeration).
     *
     * @param functionName the bound function's name as invoked in SQL.
     * @param arguments the resolved positional/named arguments.
     * @param inputSchema the Arrow schema of the input stream.
     * @param settings the session settings in effect for this bind.
     */
    public TableInOutBindParams(String functionName, Arguments arguments, Schema inputSchema,
                                 Map<String, Object> settings) {
        this(functionName, arguments, inputSchema, settings, null, false, null, null);
    }
}
