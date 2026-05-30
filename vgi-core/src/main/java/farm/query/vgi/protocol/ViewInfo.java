// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.Map;

/**
 * Mirrors the C++ {@code ViewInfoSchema}.
 */
public record ViewInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        String schema_name,
        String definition,
        Map<String, String> column_comments) implements ArrowSerializableRecord {
}
