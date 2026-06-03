// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code aggregate_bind} request, opening an aggregate execution.
 *
 * @param function_name registered aggregate function name to bind
 * @param arguments serialised Arrow batch of the positional bind-time constant arguments
 * @param input_schema serialised Arrow {@code Schema} (IPC) of the per-group input columns
 * @param settings serialised Arrow batch of session settings
 * @param secrets serialised Arrow batch of resolved secret values
 * @param attach_opaque_data per-attach state minted at catalog attach time
 */
public record AggregateBindRequest(
        String function_name,
        byte[] arguments,
        byte[] input_schema,
        byte[] settings,
        byte[] secrets,
        byte[] attach_opaque_data) implements ArrowSerializableRecord {}
