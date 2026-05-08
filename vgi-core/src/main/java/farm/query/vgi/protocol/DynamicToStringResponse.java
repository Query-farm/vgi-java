// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Response shape for {@code table_function_dynamic_to_string}: two
 * parallel string lists representing {@code keys[i] -> values[i]}. DuckDB
 * renders these as Extra Info under EXPLAIN ANALYZE.
 */
public record DynamicToStringResponse(
        List<String> keys,
        List<String> values) implements ArrowSerializableRecord {
}
