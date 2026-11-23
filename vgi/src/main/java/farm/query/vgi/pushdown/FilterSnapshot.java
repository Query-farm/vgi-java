// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;

/**
 * An init's pushdown snapshot, the value of a {@link FilterApplier}'s {@code init: StateRef}
 * (protocol 3.4): it does not change for the life of the stream, so it is stored once rather
 * than carried in every continuation token.
 *
 * @param filters the {@code InitRequest.pushdown_filters} IPC bytes, or {@code null}
 * @param join_keys the {@code InitRequest.join_keys} IPC batches
 */
public record FilterSnapshot(
        @Nullable @ArrowField(ArrowFieldType.LARGE_BINARY) byte[] filters,
        @Nullable @ArrowField(ArrowFieldType.LARGE_BINARY) List<byte[]> join_keys)
        implements ArrowSerializableRecord {
}
