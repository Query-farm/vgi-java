// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
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
 * @param path                   raw schema identifier components.
 * @param estimated_object_count per-kind object-count estimates used to gate eager
 *                               catalog loads, or {@code null} to disable gating.
 */
public record SchemaInfo(
        @Nullable String comment,
        Map<String, String> tags,
        List<String> path,
        @Nullable Map<String, Long> estimated_object_count,
        @Nullable byte[] options) implements ArrowSerializableRecord {

    /** Canonicalises the map columns to key order, so equal schemas encode to equal bytes. */
    public SchemaInfo {
        // tags / path are non-nullable on the wire: a null reads as empty.
        tags = WireMaps.sorted(tags == null ? Map.of() : tags);
        path = path == null ? List.of() : path;
        estimated_object_count = WireMaps.sorted(estimated_object_count);
    }

    /** Convenience authoring form without expression options. */
    public SchemaInfo(String comment, Map<String, String> tags, List<String> path,
                      Map<String, Long> estimatedObjectCount) {
        this(comment, tags, path, estimatedObjectCount, null);
    }

    /** Legacy author adapter: attachment context is excluded from metadata. */
    public SchemaInfo(String comment, Map<String, String> tags, byte[] ignoredAttachData, List<String> path) {
        this(comment, tags, path, null, null);
    }

    /** Legacy author adapter retaining object counts. */
    @Deprecated
    public SchemaInfo(String comment, Map<String, String> tags, byte[] ignoredAttachData,
                      List<String> path, Map<String, Long> estimatedObjectCount) {
        this(comment, tags, path, estimatedObjectCount, null);
    }

    /** Legacy author adapter retaining canonical schema options. */
    public SchemaInfo(String comment, Map<String, String> tags, byte[] ignoredAttachData,
                      List<String> path, Map<String, Long> estimatedObjectCount, byte[] options) {
        this(comment, tags, path, estimatedObjectCount, options);
    }

    /** Last path component for source compatibility with protocol 1.x callers. */
    public String name() {
        return path.isEmpty() ? "" : path.get(path.size() - 1);
    }
}
