// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record AggregateFinalizeRequest(
        String function_name,
        byte[] execution_id,
        byte[] group_ids_batch,
        byte[] output_schema,
        byte[] attach_id) implements ArrowSerializableRecord {}
