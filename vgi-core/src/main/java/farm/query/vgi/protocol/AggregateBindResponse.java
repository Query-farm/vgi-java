// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record AggregateBindResponse(
        byte[] output_schema,
        byte[] execution_id) implements ArrowSerializableRecord {}
