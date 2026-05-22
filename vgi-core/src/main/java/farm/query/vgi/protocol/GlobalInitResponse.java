// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for VGI init headers. Sent as the first batch of an {@code init}
 * stream. Mirrors {@code vgi.GlobalInitResponseWire} in vgi-go.
 */
public record GlobalInitResponse(
        byte[] execution_id,
        long max_workers,
        byte[] opaque_data) implements ArrowSerializableRecord {

    public static GlobalInitResponse of(byte[] executionId) {
        return new GlobalInitResponse(executionId, 1, null);
    }
}
