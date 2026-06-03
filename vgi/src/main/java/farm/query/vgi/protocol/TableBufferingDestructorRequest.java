// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Best-effort cleanup request for {@code table_buffering_destructor}.
 *
 * @param function_name      buffering function whose state is being released.
 * @param execution_id       execution identifier for the buffering run.
 * @param attach_opaque_data worker-private attach state.
 * @param transaction_id     enclosing transaction identifier.
 */
public record TableBufferingDestructorRequest(
        String function_name,
        byte[] execution_id,
        byte[] attach_opaque_data,
        byte[] transaction_id) implements ArrowSerializableRecord {}
