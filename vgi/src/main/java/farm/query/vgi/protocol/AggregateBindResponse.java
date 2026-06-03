// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code aggregate_bind} response.
 *
 * @param output_schema serialised Arrow {@code Schema} (IPC) of the aggregate's finalize output
 * @param execution_id opaque handle identifying this aggregate execution across subsequent
 *     update/combine/finalize/destructor calls
 */
public record AggregateBindResponse(
        byte[] output_schema,
        byte[] execution_id) implements ArrowSerializableRecord {}
