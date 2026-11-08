// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;
import java.util.List;

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
 * @param copyTo the {@code COPY ... TO} context (destination path + format) set
 *     only when this bind opens a COPY-TO sink; {@code null} otherwise. A COPY-TO
 *     writer scopes its {@code secretLookups} request to {@code copyTo.file_path()}.
 * @param argumentNames the call's argument names, or {@code null}
 * @param transactionId the plaintext id of the DuckDB transaction this call runs in (the
 *     bytes {@link farm.query.vgi.TransactionHooks#begin} returned), or {@code null} when
 *     the attach does not support transactions
 * @param attachOptions the attach's ATTACH option values, secret ones included; never
 *     {@code null}
 */
public record TableInOutBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings,
        byte[] secrets,
        boolean resolvedSecretsProvided,
        byte[] attachOpaqueData,
        farm.query.vgi.storage.BoundStorage attachStorage,
        farm.query.vgi.protocol.CopyToContext copyTo,
        List<String> argumentNames,
        byte[] transactionId,
        farm.query.vgi.AttachOptions attachOptions) {

    /** Normalizes a missing option set to the non-secret options the attach id carries. */
    public TableInOutBindParams {
        if (attachOptions == null) attachOptions = farm.query.vgi.AttachOptions.fromAttachId(attachOpaqueData);
    }

    /**
     * Compatibility constructor of releases before transaction ids and attach options.
     *
     * @param functionName the bound function's name as invoked in SQL.
     * @param arguments the resolved positional/named arguments.
     * @param inputSchema the Arrow schema of the input stream.
     * @param settings the session settings in effect for this bind.
     * @param secrets the resolved secret bytes, or {@code null}.
     * @param resolvedSecretsProvided whether resolved secrets were supplied.
     * @param attachOpaqueData the catalog attach's opaque identifier bytes, or {@code null}.
     * @param attachStorage a storage facade scoped to {@code attachOpaqueData}, or {@code null}.
     * @param copyTo the COPY-TO context, or {@code null}.
     * @param argumentNames the call's argument names, or {@code null}.
     */
    public TableInOutBindParams(String functionName, Arguments arguments, Schema inputSchema,
                                 Map<String, Object> settings, byte[] secrets,
                                 boolean resolvedSecretsProvided, byte[] attachOpaqueData,
                                 farm.query.vgi.storage.BoundStorage attachStorage,
                                 farm.query.vgi.protocol.CopyToContext copyTo, List<String> argumentNames) {
        this(functionName, arguments, inputSchema, settings, secrets, resolvedSecretsProvided,
                attachOpaqueData, attachStorage, copyTo, argumentNames, null, null);
    }

    public TableInOutBindParams(String functionName, Arguments arguments, Schema inputSchema,
                                 Map<String, Object> settings, byte[] secrets,
                                 boolean resolvedSecretsProvided, byte[] attachOpaqueData,
                                 farm.query.vgi.storage.BoundStorage attachStorage,
                                 farm.query.vgi.protocol.CopyToContext copyTo) {
        this(functionName, arguments, inputSchema, settings, secrets,
                resolvedSecretsProvided, attachOpaqueData, attachStorage, copyTo, null);
    }

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
        this(functionName, arguments, inputSchema, settings, null, false, null, null, null, null);
    }

    /**
     * Convenience constructor without the {@code copyTo} context (non-COPY binds).
     *
     * @param functionName the bound function's name as invoked in SQL.
     * @param arguments the resolved positional/named arguments.
     * @param inputSchema the Arrow schema of the input stream.
     * @param settings the session settings in effect for this bind.
     * @param secrets the resolved secret bytes, or {@code null}.
     * @param resolvedSecretsProvided whether resolved secrets were supplied.
     * @param attachOpaqueData the catalog attach's opaque identifier bytes, or {@code null}.
     * @param attachStorage a storage facade scoped to {@code attachOpaqueData}, or {@code null}.
     */
    public TableInOutBindParams(String functionName, Arguments arguments, Schema inputSchema,
                                 Map<String, Object> settings, byte[] secrets,
                                 boolean resolvedSecretsProvided, byte[] attachOpaqueData,
                                 farm.query.vgi.storage.BoundStorage attachStorage) {
        this(functionName, arguments, inputSchema, settings, secrets, resolvedSecretsProvided,
                attachOpaqueData, attachStorage, null, null);
    }
}
