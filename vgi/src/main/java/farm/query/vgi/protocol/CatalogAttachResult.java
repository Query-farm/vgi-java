// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Wire DTO for VGI {@code catalog_attach} responses. Mirrors
 * {@code vgi.CatalogAttachResultWire} in vgi-go.
 *
 * @param attach_opaque_data per-attach state echoed back on subsequent requests
 * @param supports_transactions whether the catalog implements transaction begin/commit/rollback
 * @param supports_time_travel whether the catalog can be queried at historical versions
 * @param catalog_version_frozen whether {@code catalog_version} is fixed for the attachment
 * @param catalog_version current catalog version
 * @param attach_opaque_data_required whether clients must echo {@code attach_opaque_data} back
 * @param default_schema default schema name for unqualified lookups
 * @param settings serialised Arrow batches describing exposed session settings
 * @param secret_types serialised Arrow batches describing supported secret types
 * @param comment optional human-readable catalog comment, or {@code null}
 * @param tags arbitrary key/value catalog metadata
 * @param supports_column_statistics catalog-level capability flag enabling per-column statistics
 * @param resolved_data_version data version actually selected, or {@code null}
 * @param resolved_implementation_version worker implementation version actually selected, or {@code null}
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
        List<byte[]> attach_catalogs,
        @Nullable String comment,
        Map<String, String> tags,
        boolean supports_column_statistics,
        @Nullable String resolved_data_version,
        @Nullable String resolved_implementation_version) implements ArrowSerializableRecord {
}
