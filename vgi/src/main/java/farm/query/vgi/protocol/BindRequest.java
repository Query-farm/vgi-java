// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the VGI bind request, opening a table or function binding.
 *
 * @param function_name registered function name to bind
 * @param arguments serialised Arrow batch of the positional bind-time constant arguments
 * @param function_type the function kind being bound (e.g. table, table-in-out, scalar)
 * @param input_schema serialised Arrow {@code Schema} (IPC) of the input columns, where applicable
 * @param settings serialised Arrow batch of session settings
 * @param secrets serialised Arrow batch of secret values; populated when
 *     {@code resolved_secrets_provided} is {@code true}
 * @param attach_opaque_data per-attach state minted at catalog attach time
 * @param transaction_opaque_data active transaction state, when bound inside a transaction
 * @param resolved_secrets_provided {@code true} on the second pass of the two-phase secret
 *     exchange, after DuckDB has resolved the secrets requested by an earlier {@link BindResponse}
 */
public record BindRequest(
        String function_name,
        byte[] arguments,
        String function_type,
        byte[] input_schema,
        byte[] settings,
        byte[] secrets,
        byte[] attach_opaque_data,
        byte[] transaction_opaque_data,
        boolean resolved_secrets_provided) implements ArrowSerializableRecord {}
