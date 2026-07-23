// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Wire DTO for the {@code aggregate_combine} request, merging partial aggregate states.
 *
 * @param function_name registered aggregate function name
 * @param execution_id execution handle from {@link AggregateBindResponse}
 * @param merge_batch serialised Arrow batch mapping source group states into target groups
 * @param attach_opaque_data per-attach state minted at catalog attach time
 * @param schema_name catalog schema that declares the function. A function name is unique only
 *     within a schema, so this is what lets the worker resolve {@code (schema, name)} rather than
 *     running whichever same-named implementation the by-name lookup finds first. {@code null}
 *     when the caller names no schema. Additive, nullable, name-keyed wire field; protocol 1.2.0
 */
public record AggregateCombineRequest(
        String function_name,
        byte[] execution_id,
        byte[] merge_batch,
        byte[] attach_opaque_data,
        @Nullable String schema_name) implements ArrowSerializableRecord {}
