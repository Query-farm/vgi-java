// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Best-effort cleanup request for {@code table_buffering_destructor}.
 *
 * @param function_name      buffering function whose state is being released.
 * @param execution_id       execution identifier for the buffering run.
 * @param attach_opaque_data worker-private attach state.
 * @param transaction_id     enclosing transaction identifier.
 * @param schema_name catalog schema that declares the function. A function name is unique only
 *     within a schema, so this is what lets the worker resolve {@code (schema, name)} rather than
 *     running whichever same-named implementation the by-name lookup finds first. {@code null}
 *     when the caller names no schema. Additive, nullable, name-keyed wire field; protocol 1.2.0
 */
public record TableBufferingDestructorRequest(
        String function_name,
        byte[] execution_id,
        byte[] attach_opaque_data,
        byte[] transaction_id,
        @Nullable String schema_name) implements ArrowSerializableRecord {}
