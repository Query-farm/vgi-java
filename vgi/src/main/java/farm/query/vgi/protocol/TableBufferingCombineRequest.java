// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Combine-phase request for {@code table_buffering_combine} — once per query
 * after all input. Inner field order matches C++ {@code BuildTableBufferingCombineInner}.
 *
 * @param function_name      buffering function being combined.
 * @param execution_id       execution identifier for the buffering run.
 * @param state_ids          per-sink state identifiers produced by the process phase.
 * @param attach_opaque_data worker-private attach state.
 * @param transaction_id     enclosing transaction identifier.
 */
public record TableBufferingCombineRequest(
        String function_name,
        byte[] execution_id,
        List<byte[]> state_ids,
        byte[] attach_opaque_data,
        byte[] transaction_id) implements ArrowSerializableRecord {}
