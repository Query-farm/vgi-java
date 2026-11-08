// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;
import java.util.List;

/**
 * Parameters passed to {@link TableFunction#onBind} and the other bind-time
 * hooks ({@code cardinality}, {@code statistics}). Decoded from the wire
 * {@code BindRequest}.
 *
 * @param functionName the invoked table function's name
 * @param arguments the positional and named call arguments
 * @param inputSchema the schema of any table-valued input, or empty when none
 * @param settings session settings in effect for this call
 * @param secrets the resolved secret bytes, or {@code null} when none were sent
 * @param resolvedSecretsProvided whether the client supplied resolved secrets
 * @param attachId the catalog attach's opaque identifier bytes
 * @param transactionStorage per-transaction storage, or {@code null} outside an
 *     explicit {@code BEGIN}/{@code COMMIT} block (autocommit — no caching)
 * @param attachStorage a storage facade scoped to {@code attachId} — state that
 *     persists across queries within one ATTACH session; {@code null} when the
 *     bind has no attach context (catalog enumeration, cardinality/statistics)
 * @param copyFrom the {@code COPY ... FROM} context, set only when this bind opens a
 *     COPY-FROM scan (see {@link farm.query.vgi.table.CopyFromFunction}); {@code null}
 *     for every ordinary table-function bind
 * @param argumentNames the call's argument names, or {@code null}
 * @param transactionId the plaintext id of the DuckDB transaction this call runs in, exactly
 *     as {@link farm.query.vgi.TransactionHooks#begin} returned it (the framework opens the
 *     sealed wire value on HTTP), or {@code null} when the attach does not support
 *     transactions; see {@link farm.query.vgi.TransactionHooks}
 * @param attachOptions the attach's ATTACH option values, secret ones included; never
 *     {@code null} ({@link farm.query.vgi.AttachOptions#empty()} without options)
 */
public record TableBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings,
        byte[] secrets,
        boolean resolvedSecretsProvided,
        byte[] attachId,
        TransactionStorage transactionStorage,
        farm.query.vgi.storage.BoundStorage attachStorage,
        farm.query.vgi.protocol.CopyFromContext copyFrom,
        List<String> argumentNames,
        byte[] transactionId,
        farm.query.vgi.AttachOptions attachOptions) {

    /** Normalizes a missing option set to the non-secret options the attach id carries. */
    public TableBindParams {
        if (attachOptions == null) attachOptions = farm.query.vgi.AttachOptions.fromAttachId(attachId);
    }

    /**
     * Compatibility constructor of releases before transaction ids and attach options.
     *
     * @param functionName the invoked table function's name
     * @param arguments the positional and named call arguments
     * @param inputSchema the schema of any table-valued input
     * @param settings session settings in effect for this call
     * @param secrets the resolved secret bytes, or {@code null}
     * @param resolvedSecretsProvided whether the client supplied resolved secrets
     * @param attachId the catalog attach's opaque identifier bytes
     * @param transactionStorage per-transaction storage, or {@code null}
     * @param attachStorage attach-scoped storage, or {@code null}
     * @param copyFrom the COPY-FROM context, or {@code null}
     * @param argumentNames the call's argument names, or {@code null}
     */
    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                           Map<String, Object> settings, byte[] secrets,
                           boolean resolvedSecretsProvided, byte[] attachId,
                           TransactionStorage transactionStorage,
                           farm.query.vgi.storage.BoundStorage attachStorage,
                           farm.query.vgi.protocol.CopyFromContext copyFrom,
                           List<String> argumentNames) {
        this(functionName, arguments, inputSchema, settings, secrets, resolvedSecretsProvided,
                attachId, transactionStorage, attachStorage, copyFrom, argumentNames, null, null);
    }

    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                           Map<String, Object> settings, byte[] secrets,
                           boolean resolvedSecretsProvided, byte[] attachId,
                           TransactionStorage transactionStorage,
                           farm.query.vgi.storage.BoundStorage attachStorage,
                           farm.query.vgi.protocol.CopyFromContext copyFrom) {
        this(functionName, arguments, inputSchema, settings, secrets,
                resolvedSecretsProvided, attachId, transactionStorage, attachStorage,
                copyFrom, null);
    }

    /**
     * Convenience constructor with no secrets, attach id, or transaction storage.
     *
     * @param functionName the invoked table function's name
     * @param arguments the positional and named call arguments
     * @param inputSchema the schema of any table-valued input
     * @param settings session settings in effect for this call
     */
    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                            Map<String, Object> settings) {
        this(functionName, arguments, inputSchema, settings, null, false, null, null, null, null, null);
    }

    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                            Map<String, Object> settings, List<String> argumentNames) {
        this(functionName, arguments, inputSchema, settings, null, false, null, null, null, null,
                argumentNames);
    }

    /**
     * Convenience constructor with secrets but no attach id or transaction storage.
     *
     * @param functionName the invoked table function's name
     * @param arguments the positional and named call arguments
     * @param inputSchema the schema of any table-valued input
     * @param settings session settings in effect for this call
     * @param secrets the resolved secret bytes, or {@code null}
     * @param resolvedSecretsProvided whether the client supplied resolved secrets
     */
    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                            Map<String, Object> settings, byte[] secrets,
                            boolean resolvedSecretsProvided) {
        this(functionName, arguments, inputSchema, settings, secrets, resolvedSecretsProvided,
                null, null, null, null, null);
    }

    /**
     * Convenience constructor with secrets and attach id but no transaction storage.
     *
     * @param functionName the invoked table function's name
     * @param arguments the positional and named call arguments
     * @param inputSchema the schema of any table-valued input
     * @param settings session settings in effect for this call
     * @param secrets the resolved secret bytes, or {@code null}
     * @param resolvedSecretsProvided whether the client supplied resolved secrets
     * @param attachId the catalog attach's opaque identifier bytes
     */
    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                            Map<String, Object> settings, byte[] secrets,
                            boolean resolvedSecretsProvided, byte[] attachId) {
        this(functionName, arguments, inputSchema, settings, secrets, resolvedSecretsProvided,
                attachId, null, null, null, null);
    }
}
