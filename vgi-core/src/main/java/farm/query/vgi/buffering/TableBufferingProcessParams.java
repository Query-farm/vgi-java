// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.buffering;

import farm.query.vgirpc.CallContext;

/**
 * Context passed to {@link TableBufferingFunction#process}: the execution's
 * storage view, its {@code execution_id}, the optional per-batch index
 * (present only when {@code requires_input_batch_index} is declared), and the
 * call context — {@code ctx.clientLog(...)} surfaces in {@code duckdb_logs()}.
 */
public record TableBufferingProcessParams(
        String functionName,
        byte[] executionId,
        BufferingStorage storage,
        Long batchIndex,
        CallContext ctx) {}
