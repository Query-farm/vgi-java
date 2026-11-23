// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.ArgumentsLocation;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * What a scalar stream needs from its init on every turn, carried as one
 * {@code init: StateRef} (vgi large-values spec §6) instead of separate cursor fields.
 *
 * <p>Built once, in the init handler. The arguments are carried in the form the bind call
 * received them: inline IPC, or the argument reference (never its resolved value), which each
 * turn resolves through vgi-rpc's object cache.
 *
 * @param output_schema the declared output schema IPC
 * @param arguments the inline arguments IPC, or {@code null} when they came by reference
 * @param arguments_location the argument reference, or {@code null} when inline
 * @param settings the session settings IPC
 * @param secrets the resolved secret values IPC, or {@code null}
 */
public record ScalarStreamInit(
        byte[] output_schema,
        @Nullable byte[] arguments,
        @Nullable ArgumentsLocation arguments_location,
        @Nullable byte[] settings,
        @Nullable byte[] secrets) implements ArrowSerializableRecord {
}
