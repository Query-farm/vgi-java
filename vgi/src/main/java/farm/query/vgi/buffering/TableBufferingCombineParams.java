// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.buffering;

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
 */
public record TableBufferingCombineParams(
        String functionName,
        byte[] executionId,
        BufferingStorage storage,
        CallContext ctx) {}
