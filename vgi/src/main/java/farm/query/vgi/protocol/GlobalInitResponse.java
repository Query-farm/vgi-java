// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for VGI init headers. Sent as the first batch of an {@code init}
 * stream. Mirrors {@code vgi.GlobalInitResponseWire} in vgi-go.
 *
 * @param execution_id worker-minted execution identifier for the bound query.
 * @param max_workers  maximum number of parallel workers the client may use.
 * @param opaque_data  worker-private state echoed back on later RPCs, or {@code null}.
 */
public record GlobalInitResponse(
        byte[] execution_id,
        long max_workers,
        byte[] opaque_data) implements ArrowSerializableRecord {

    /**
     * Builds a single-worker response with no opaque data.
     *
     * @param executionId the execution identifier to advertise.
     * @return a response with {@code max_workers == 1} and {@code null} opaque data.
     */
    public static GlobalInitResponse of(byte[] executionId) {
        return new GlobalInitResponse(executionId, 1, null);
    }
}
