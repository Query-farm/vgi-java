// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Wire DTO for the {@code aggregate_destructor} request, releasing per-group aggregate state.
 *
 * @param function_name registered aggregate function name
 * @param execution_id execution handle from {@link AggregateBindResponse}
 * @param group_ids_batch serialised Arrow batch of the group ids whose state may be freed
 * @param attach_opaque_data per-attach state minted at catalog attach time
 * @param schema_name catalog schema that declares the function. A function name is unique only
 *     within a schema, so this is what lets the worker resolve {@code (schema, name)} rather than
 *     running whichever same-named implementation the by-name lookup finds first. {@code null}
 *     when the caller names no schema. Additive, nullable, name-keyed wire field; protocol 1.2.0
 */
public record AggregateDestructorRequest(
        String function_name,
        byte[] execution_id,
        byte[] group_ids_batch,
        byte[] attach_opaque_data,
        @Nullable String schema_name) implements ArrowSerializableRecord {}
