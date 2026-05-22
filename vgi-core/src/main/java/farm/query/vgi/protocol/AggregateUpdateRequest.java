// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record AggregateUpdateRequest(
        String function_name,
        byte[] execution_id,
        byte[] input_batch,
        byte[] attach_opaque_data) implements ArrowSerializableRecord {}
