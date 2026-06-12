// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Buffering-function init metadata, persisted into execution-scoped storage at
 * Sink init ({@code FrameworkNs.BUFFERING_INIT}, key {@code packIntKey(-1)})
 * and cold-loaded by every {@code table_buffering_process}/{@code _combine}
 * RPC — mirrors vgi-python's {@code _encode_table_buffering_init}, so any pool
 * worker can serve any RPC for any execution_id without in-process bind state.
 *
 * @param arguments     the bind request's IPC-encoded arguments
 * @param settings      the bind request's IPC-encoded settings
 * @param output_schema the bound output schema's IPC bytes, or {@code null}
 * @param attach_plain  the unsealed attach plaintext (the cross-query storage scope)
 */
public record BufferingInitState(
        byte[] arguments,
        byte[] settings,
        byte[] output_schema,
        byte[] attach_plain) implements ArrowSerializableRecord {}
