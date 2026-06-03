// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Response from {@code table_buffering_combine} — the finalize partition keys.
 *
 * @param finalize_state_ids state identifiers, one per finalize/source partition to drain.
 */
public record TableBufferingCombineResponse(
        List<byte[]> finalize_state_ids) implements ArrowSerializableRecord {}
