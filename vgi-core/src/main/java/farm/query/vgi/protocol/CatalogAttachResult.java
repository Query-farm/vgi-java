// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Wire DTO for VGI catalog_attach responses. Mirrors
 * {@code vgi.CatalogAttachResultWire} in vgi-go.
 */
public record CatalogAttachResult(
        byte[] attach_opaque_data,
        boolean supports_transactions,
        boolean supports_time_travel,
        boolean catalog_version_frozen,
        long catalog_version,
        boolean attach_opaque_data_required,
        String default_schema,
        List<byte[]> settings,
        List<byte[]> secret_types,
        @Nullable String comment,
        Map<String, String> tags,
        boolean supports_column_statistics,
        @Nullable String resolved_data_version,
        @Nullable String resolved_implementation_version) implements ArrowSerializableRecord {
}
