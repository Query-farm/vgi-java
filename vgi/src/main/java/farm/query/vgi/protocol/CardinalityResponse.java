// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Wire shape for the {@code table_function_cardinality} RPC reply.
 *
 * <p>Both fields are nullable on the wire; {@code null} means "unknown" — DuckDB
 * falls back to its own default.</p>
 *
 * @param estimate expected row count, or {@code null} if unknown
 * @param max upper-bound row count, or {@code null} if unknown
 */
public record CardinalityResponse(
        @Nullable Long estimate,
        @Nullable Long max) implements ArrowSerializableRecord {
}
