// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Sink-phase request for {@code table_buffering_process} — one input batch.
 * Inner field order/types match the C++ {@code BuildTableBufferingProcessInner}.
 *
 * @param function_name      buffering function consuming the batch.
 * @param execution_id       execution identifier for the buffering run.
 * @param input_batch        IPC-encoded input record batch.
 * @param attach_opaque_data worker-private attach state.
 * @param transaction_id     enclosing transaction identifier.
 * @param batch_index        monotone batch index, or {@code null} when not required.
 */
public record TableBufferingProcessRequest(
        String function_name,
        byte[] execution_id,
        byte[] input_batch,
        byte[] attach_opaque_data,
        byte[] transaction_id,
        Long batch_index) implements ArrowSerializableRecord {}
