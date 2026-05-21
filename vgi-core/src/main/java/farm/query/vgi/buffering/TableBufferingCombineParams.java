// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.buffering;

import farm.query.vgirpc.CallContext;

/**
 * Context passed to {@link TableBufferingFunction#combine}: the execution's
 * storage view, {@code execution_id}, and the call context for
 * {@code ctx.clientLog(...)}.
 */
public record TableBufferingCombineParams(
        String functionName,
        byte[] executionId,
        BufferingStorage storage,
        CallContext ctx) {}
