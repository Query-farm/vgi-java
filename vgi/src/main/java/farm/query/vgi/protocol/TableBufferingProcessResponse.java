// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Response from {@code table_buffering_process} — the worker-chosen state_id.
 *
 * @param state_id identifier of the sink state that accepted the batch, echoed
 *                 back to the combine phase via {@link TableBufferingCombineRequest#state_ids}.
 */
public record TableBufferingProcessResponse(byte[] state_id) implements ArrowSerializableRecord {}
