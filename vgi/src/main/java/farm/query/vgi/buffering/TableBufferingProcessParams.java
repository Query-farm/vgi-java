// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.buffering;

import farm.query.vgi.storage.BoundStorage;
import farm.query.vgirpc.CallContext;

/**
 * Context passed to {@link TableBufferingFunction#process}: the execution's
 * storage view, its {@code execution_id}, the optional per-batch index
 * (present only when {@code requires_input_batch_index} is declared), and the
 * call context — {@code ctx.clientLog(...)} surfaces in {@code duckdb_logs()}.
 *
 * @param functionName the bound function's name.
 * @param executionId the opaque {@code execution_id} identifying this buffering execution.
 * @param storage the storage view bound to {@code executionId}, for stashing the batch.
 * @param batchIndex DuckDB's globally-unique batch index, or {@code null} unless {@code requires_input_batch_index} was declared.
 * @param ctx the RPC call context; {@code ctx.clientLog(...)} surfaces in {@code duckdb_logs()}.
 */
public record TableBufferingProcessParams(
        String functionName,
        byte[] executionId,
        BoundStorage storage,
        Long batchIndex,
        CallContext ctx) {}
