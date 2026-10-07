// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Wire DTO for VGI init headers. Sent as the first batch of an {@code init}
 * stream. Mirrors {@code vgi.GlobalInitResponseWire} in vgi-go.
 *
 * <p>Component order is the wire field order of the {@code init} stream header,
 * and therefore part of the {@code vgi.v2} protocol hash: it must stay
 * {@code execution_id, opaque_data, max_workers}, the reference's generated
 * schema order.
 *
 * @param execution_id worker-minted execution identifier for the bound query.
 * @param opaque_data  worker-private state echoed back on later RPCs, or {@code null}.
 * @param max_workers  maximum number of parallel workers the client may use.
 */
public record GlobalInitResponse(
        byte[] execution_id,
        @Nullable byte[] opaque_data,
        long max_workers) implements ArrowSerializableRecord {

    /**
     * Builds a single-worker response with no opaque data.
     *
     * @param executionId the execution identifier to advertise.
     * @return a response with {@code max_workers == 1} and {@code null} opaque data.
     */
    public static GlobalInitResponse of(byte[] executionId) {
        return new GlobalInitResponse(executionId, null, 1);
    }
}
