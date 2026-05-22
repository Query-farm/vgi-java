// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.Map;

/**
 * Mirrors the C++ {@code SchemaInfoSchema}.
 *
 * <p>{@code estimated_object_count} is consulted by the C++ extension to gate
 * eager catalog loads — entries with count {@code 0} for a kind ({@code
 * tables} / {@code views} / {@code macros} / {@code functions} / {@code
 * indexes}) skip the corresponding {@code catalog_schema_contents_*} RPC
 * entirely, so workers with no objects of a kind avoid round-trips.</p>
 */
public record SchemaInfo(
        @Nullable String comment,
        Map<String, String> tags,
        byte[] attach_opaque_data,
        String name,
        @Nullable Map<String, Long> estimated_object_count) implements ArrowSerializableRecord {

    /** Convenience: no estimated_object_count map (eager-load gating disabled). */
    public SchemaInfo(String comment, Map<String, String> tags, byte[] attach_opaque_data, String name) {
        this(comment, tags, attach_opaque_data, name, null);
    }
}
