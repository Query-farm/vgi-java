// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record AggregateCombineRequest(
        String function_name,
        byte[] execution_id,
        byte[] merge_batch,
        byte[] attach_opaque_data) implements ArrowSerializableRecord {}
