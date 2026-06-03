// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Response shape for {@code table_function_dynamic_to_string}: two
 * parallel string lists representing {@code keys[i] -> values[i]}.
 *
 * <p>DuckDB renders these as Extra Info under {@code EXPLAIN ANALYZE}.</p>
 *
 * @param keys the entry keys
 * @param values the entry values, parallel to {@code keys}
 */
public record DynamicToStringResponse(
        List<String> keys,
        List<String> values) implements ArrowSerializableRecord {
}
