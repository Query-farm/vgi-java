// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

public record TableScanFunctionGetResponse(
        String function_name,
        byte[] arguments,
        List<String> required_extensions) implements ArrowSerializableRecord {
}
