// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;

/**
 * Combine-phase request for {@code table_buffering_combine} — once per query
 * after all input. Inner field order matches C++ {@code BuildTableBufferingCombineInner}.
 *
 * @param function_name      buffering function being combined.
 * @param execution_id       execution identifier for the buffering run.
 * @param state_ids          per-sink state identifiers produced by the process phase.
 * @param attach_opaque_data worker-private attach state.
 * @param transaction_id     enclosing transaction identifier.
 * @param schema_name catalog schema that declares the function. A function name is unique only
 *     within a schema, so this is what lets the worker resolve {@code (schema, name)} rather than
 *     running whichever same-named implementation the by-name lookup finds first. {@code null}
 *     when the caller names no schema. Additive, nullable, name-keyed wire field; protocol 1.2.0
 */
public record TableBufferingCombineRequest(
        String function_name,
        byte[] execution_id,
        List<byte[]> state_ids,
        byte[] attach_opaque_data,
        byte[] transaction_id,
        @Nullable String schema_name) implements ArrowSerializableRecord {}
