// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code table_function_cardinality} request.
 *
 * @param bind_call serialised bind call that identifies the binding being estimated
 * @param bind_opaque_data bind-time opaque state returned by the corresponding {@link BindResponse}
 */
public record CardinalityRequest(
        byte[] bind_call,
        byte[] bind_opaque_data) implements ArrowSerializableRecord {}
