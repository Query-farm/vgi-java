// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

public record InitRequest(
        byte[] bind_call,
        byte[] output_schema,
        byte[] bind_opaque_data,
        List<Integer> projection_ids,
        byte[] pushdown_filters,
        List<byte[]> join_keys,
        String phase,
        byte[] execution_id,
        byte[] init_opaque_data,
        String order_by_column_name,
        String order_by_direction,
        String order_by_null_order,
        Long order_by_limit,
        Double tablesample_percentage,
        Long tablesample_seed,
        byte[] finalize_state_id) implements ArrowSerializableRecord {}
