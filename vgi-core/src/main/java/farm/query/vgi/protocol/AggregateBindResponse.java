// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record AggregateBindResponse(
        byte[] output_schema,
        byte[] execution_id) implements ArrowSerializableRecord {}
