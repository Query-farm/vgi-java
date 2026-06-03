// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.Map;

/**
 * Mirrors the C++ {@code ViewInfoSchema}.
 *
 * @param comment         optional view comment, or {@code null}.
 * @param tags            arbitrary key/value metadata tags.
 * @param name            view name.
 * @param schema_name     owning schema name.
 * @param definition      the view's SQL definition.
 * @param column_comments per-column comments keyed by column name.
 */
public record ViewInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        String schema_name,
        String definition,
        Map<String, String> column_comments) implements ArrowSerializableRecord {
}
