// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

public record TableBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings,
        byte[] secrets,
        boolean resolvedSecretsProvided,
        byte[] attachId,
        /** Per-transaction storage, or {@code null} outside an explicit
         *  {@code BEGIN}/{@code COMMIT} block (autocommit — no caching). */
        TransactionStorage transactionStorage) {

    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                            Map<String, Object> settings) {
        this(functionName, arguments, inputSchema, settings, null, false, null, null);
    }

    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                            Map<String, Object> settings, byte[] secrets,
                            boolean resolvedSecretsProvided) {
        this(functionName, arguments, inputSchema, settings, secrets, resolvedSecretsProvided, null, null);
    }

    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                            Map<String, Object> settings, byte[] secrets,
                            boolean resolvedSecretsProvided, byte[] attachId) {
        this(functionName, arguments, inputSchema, settings, secrets, resolvedSecretsProvided,
                attachId, null);
    }
}
