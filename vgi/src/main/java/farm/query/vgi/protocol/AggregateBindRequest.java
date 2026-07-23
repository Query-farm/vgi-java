// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Wire DTO for the {@code aggregate_bind} request, opening an aggregate execution.
 *
 * @param function_name registered aggregate function name to bind
 * @param arguments serialised Arrow batch of the positional bind-time constant arguments
 * @param input_schema serialised Arrow {@code Schema} (IPC) of the per-group input columns
 * @param settings serialised Arrow batch of session settings
 * @param secrets serialised Arrow batch of resolved secret values
 * @param attach_opaque_data per-attach state minted at catalog attach time
 * @param schema_name catalog schema that declares the function. A function name is unique only
 *     within a schema, so this is what lets the worker resolve {@code (schema, name)} rather than
 *     running whichever same-named implementation the by-name lookup finds first. {@code null}
 *     when the caller names no schema. Additive, nullable, name-keyed wire field; protocol 1.2.0
 */
public record AggregateBindRequest(
        String function_name,
        byte[] arguments,
        byte[] input_schema,
        byte[] settings,
        byte[] secrets,
        byte[] attach_opaque_data,
        @Nullable String schema_name) implements ArrowSerializableRecord {}
