// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code aggregate_finalize} request, producing per-group result rows.
 *
 * @param function_name registered aggregate function name
 * @param execution_id execution handle from {@link AggregateBindResponse}
 * @param group_ids_batch serialised Arrow batch of the group ids to finalize, in result order
 * @param output_schema serialised Arrow {@code Schema} (IPC) the result batch must conform to
 * @param attach_opaque_data per-attach state minted at catalog attach time
 */
public record AggregateFinalizeRequest(
        String function_name,
        byte[] execution_id,
        byte[] group_ids_batch,
        byte[] output_schema,
        byte[] attach_opaque_data) implements ArrowSerializableRecord {}
