// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record TableScanFunctionGetRequest(
        byte[] attach_id,
        String schema_name,
        String name,
        String at_unit,
        String at_value,
        byte[] transaction_id) implements ArrowSerializableRecord {}
