// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code aggregate_destructor} request, releasing per-group aggregate state.
 *
 * @param function_name registered aggregate function name
 * @param execution_id execution handle from {@link AggregateBindResponse}
 * @param group_ids_batch serialised Arrow batch of the group ids whose state may be freed
 * @param attach_opaque_data per-attach state minted at catalog attach time
 */
public record AggregateDestructorRequest(
        String function_name,
        byte[] execution_id,
        byte[] group_ids_batch,
        byte[] attach_opaque_data) implements ArrowSerializableRecord {}
