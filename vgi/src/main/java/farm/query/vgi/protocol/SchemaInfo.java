// Copyright 2026 Query Farm LLC - https://query.farm

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
 *
 * @param comment                optional schema comment, or {@code null}.
 * @param tags                   arbitrary key/value metadata tags.
 * @param attach_opaque_data     worker-private attach state for this schema.
 * @param name                   schema name.
 * @param estimated_object_count per-kind object-count estimates used to gate eager
 *                               catalog loads, or {@code null} to disable gating.
 */
public record SchemaInfo(
        @Nullable String comment,
        Map<String, String> tags,
        byte[] attach_opaque_data,
        String name,
        @Nullable Map<String, Long> estimated_object_count) implements ArrowSerializableRecord {

    /**
     * Convenience constructor with no {@code estimated_object_count} map
     * (eager-load gating disabled).
     *
     * @param comment            optional schema comment, or {@code null}.
     * @param tags               arbitrary key/value metadata tags.
     * @param attach_opaque_data worker-private attach state for this schema.
     * @param name               schema name.
     */
    public SchemaInfo(String comment, Map<String, String> tags, byte[] attach_opaque_data, String name) {
        this(comment, tags, attach_opaque_data, name, null);
    }
}
