// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code catalog_version} response.
 *
 * @param version the catalog's current version number
 */
public record CatalogVersionResponse(long version) implements ArrowSerializableRecord {}
