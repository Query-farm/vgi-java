// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.CopyToContext;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Buffering-function init metadata, persisted into execution-scoped storage at
 * Sink init ({@code FrameworkNs.BUFFERING_INIT}, key {@code packIntKey(-1)})
 * and cold-loaded by every {@code table_buffering_process}/{@code _combine}
 * RPC — mirrors vgi-python's {@code _encode_table_buffering_init}, so any pool
 * worker can serve any RPC for any execution_id without in-process bind state.
 *
 * <p>It carries the resolved secret values, so it is only ever stored sealed
 * ({@link StoragePayloadSeal}). The attach is deliberately not part of it:
 * process/combine always use the request's own authenticated attach.
 *
 * @param arguments     the bind request's IPC-encoded arguments, or {@code null} when they came
 *     by reference (protocol 3.4); never the resolved value of a reference
 * @param settings      the bind request's IPC-encoded settings
 * @param output_schema the bound output schema's IPC bytes, or {@code null}
 * @param input_schema  the bind request's source input schema IPC bytes, or {@code null};
 *     COPY-TO writers read it in {@code close()} to write a header for an empty source
 * @param copy_to       the {@code COPY ... TO} context (destination path + format) when this
 *     buffering execution backs a COPY-TO sink, or {@code null} for ordinary buffering
 * @param secrets       the bind request's resolved-secrets IPC bytes, or {@code null};
 *     replayed onto the process/combine params so a COPY-TO writer's
 *     {@code write()}/{@code close()} can read forwarded {@code CREATE SECRET} creds
 * @param arguments_location the bind request's argument reference, or {@code null} when the
 *     arguments came inline; process/combine resolve it through their turn's context
 */
public record BufferingInitState(
        @Nullable byte[] arguments,
        byte[] settings,
        byte[] output_schema,
        @Nullable byte[] input_schema,
        @Nullable CopyToContext copy_to,
        @Nullable byte[] secrets,
        @Nullable farm.query.vgi.protocol.ArgumentsLocation arguments_location) implements ArrowSerializableRecord {}
