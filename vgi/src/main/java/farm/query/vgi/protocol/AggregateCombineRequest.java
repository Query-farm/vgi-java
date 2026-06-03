// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code aggregate_combine} request, merging partial aggregate states.
 *
 * @param function_name registered aggregate function name
 * @param execution_id execution handle from {@link AggregateBindResponse}
 * @param merge_batch serialised Arrow batch mapping source group states into target groups
 * @param attach_opaque_data per-attach state minted at catalog attach time
 */
public record AggregateCombineRequest(
        String function_name,
        byte[] execution_id,
        byte[] merge_batch,
        byte[] attach_opaque_data) implements ArrowSerializableRecord {}
