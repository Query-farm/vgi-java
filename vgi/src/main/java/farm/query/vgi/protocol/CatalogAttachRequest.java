// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Wire DTO for the {@code catalog_attach} request, opening a catalog attachment.
 *
 * @param name catalog name being attached
 * @param options serialised Arrow batch of ATTACH options
 * @param data_version_spec requested data-version constraint (e.g. a semver range), or empty
 * @param implementation_version requested worker implementation version, or empty
 * @param client_capabilities serialised Arrow batch describing the calling engine —
 *        its name, the formats and catalogs it can bind natively, whether it can
 *        stream, and which filter encodings it speaks. Empty from a client that
 *        declares nothing
 */
public record CatalogAttachRequest(
        String name,
        byte[] options,
        @Nullable String data_version_spec,
        @Nullable String implementation_version,
        byte[] client_capabilities) implements ArrowSerializableRecord {

    /**
     * An attach request from a client that declares no capabilities.
     *
     * @param name catalog name being attached.
     * @param options serialised Arrow batch of ATTACH options.
     * @param dataVersionSpec requested data-version constraint, or empty.
     * @param implementationVersion requested worker implementation version, or empty.
     * @return the request, with empty {@code client_capabilities}.
     */
    public static CatalogAttachRequest of(String name, byte[] options, String dataVersionSpec,
            String implementationVersion) {
        return new CatalogAttachRequest(name, options, dataVersionSpec, implementationVersion,
                new byte[0]);
    }
}
