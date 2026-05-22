// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record CardinalityRequest(
        byte[] bind_call,
        byte[] bind_opaque_data) implements ArrowSerializableRecord {}
