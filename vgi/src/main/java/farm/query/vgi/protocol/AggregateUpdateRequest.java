// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code aggregate_update} request, feeding a batch of input rows into groups.
 *
 * @param function_name registered aggregate function name
 * @param execution_id execution handle from {@link AggregateBindResponse}
 * @param input_batch serialised Arrow batch of input rows (with group ids) to accumulate
 * @param attach_opaque_data per-attach state minted at catalog attach time
 */
public record AggregateUpdateRequest(
        String function_name,
        byte[] execution_id,
        byte[] input_batch,
        byte[] attach_opaque_data) implements ArrowSerializableRecord {}
