// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Sink-phase request for {@code table_buffering_process} — one input batch.
 * Inner field order/types match the C++ {@code BuildTableBufferingProcessInner}.
 */
public record TableBufferingProcessRequest(
        String function_name,
        byte[] execution_id,
        byte[] input_batch,
        byte[] attach_opaque_data,
        byte[] transaction_id,
        Long batch_index) implements ArrowSerializableRecord {}
