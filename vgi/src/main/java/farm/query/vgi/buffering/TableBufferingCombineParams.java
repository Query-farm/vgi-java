// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.buffering;

import farm.query.vgi.storage.BoundStorage;
import farm.query.vgirpc.CallContext;

/**
 * Context passed to {@link TableBufferingFunction#combine}: the execution's
 * storage view, {@code execution_id}, and the call context for
 * {@code ctx.clientLog(...)}.
 *
 * @param functionName the bound function's name.
 * @param executionId the opaque {@code execution_id} identifying this buffering execution.
 * @param storage the storage view bound to {@code executionId}, for reading back stashed state.
 * @param ctx the RPC call context; {@code ctx.clientLog(...)} surfaces in {@code duckdb_logs()}.
 * @param args the bind-time arguments, rehydrated from the init metadata
 *     persisted in storage (any pool worker can serve the RPC).
 * @param outputSchema the bound output schema, rehydrated the same way.
 * @param attachOpaqueData the catalog attach's opaque identifier bytes —
 *     {@code storage().rescope(attachOpaqueData())} reaches state that
 *     persists across queries within one ATTACH session.
 * @param inputSchema the source input schema, rehydrated the same way; {@code null}
 *     when the bind carried no input schema.
 * @param copyTo the {@code COPY ... TO} context (destination path + format) when this
 *     buffering execution backs a COPY-TO sink, or {@code null} for ordinary buffering.
 * @param secrets the resolved-secrets IPC bytes forwarded via the two-phase secret
 *     bind, or {@code null}; a COPY-TO writer parses them via {@code Secrets.parse}.
 */
public record TableBufferingCombineParams(
        String functionName,
        byte[] executionId,
        BoundStorage storage,
        CallContext ctx,
        farm.query.vgi.function.Arguments args,
        org.apache.arrow.vector.types.pojo.Schema outputSchema,
        byte[] attachOpaqueData,
        org.apache.arrow.vector.types.pojo.Schema inputSchema,
        farm.query.vgi.protocol.CopyToContext copyTo,
        byte[] secrets) {}
