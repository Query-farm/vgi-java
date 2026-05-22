// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

public record TableScanFunctionGetResponse(
        String function_name,
        byte[] arguments,
        List<String> required_extensions) implements ArrowSerializableRecord {
}
