// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Wire shape for the {@code table_function_cardinality} RPC reply. Both
 * fields are nullable on the wire; {@code null} means "unknown" — DuckDB
 * falls back to its own default.
 */
public record CardinalityResponse(
        @Nullable Long estimate,
        @Nullable Long max) implements ArrowSerializableRecord {
}
