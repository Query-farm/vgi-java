// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/** Response from {@code table_buffering_process} — the worker-chosen state_id. */
public record TableBufferingProcessResponse(byte[] state_id) implements ArrowSerializableRecord {}
