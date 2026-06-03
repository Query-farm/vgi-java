// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code catalog_attach} request, opening a catalog attachment.
 *
 * @param name catalog name being attached
 * @param options serialised Arrow batch of ATTACH options
 * @param data_version_spec requested data-version constraint (e.g. a semver range), or empty
 * @param implementation_version requested worker implementation version, or empty
 */
public record CatalogAttachRequest(
        String name,
        byte[] options,
        String data_version_spec,
        String implementation_version) implements ArrowSerializableRecord {}
