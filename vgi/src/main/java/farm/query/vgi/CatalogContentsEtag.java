// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

/**
 * The framework's etag policy for {@code catalog_contents}, set with
 * {@link Worker#catalogContentsEtag(CatalogContentsEtag)}.
 *
 * <p>Mirrors vgi-python's {@code CatalogInterface.catalog_contents_etag}.</p>
 */
public enum CatalogContentsEtag {

    /** No etag unless the catalog's {@link CatalogContentsProvider} returns one (the default). */
    NONE,

    /**
     * When the catalog returns no etag of its own, use the hex SHA-256 of the
     * snapshot ({@link CatalogContents#digest}) and turn a matching
     * {@code if_none_match} into {@code not_modified}. The snapshot is still
     * built on every call, but the transfer and the client's decode are saved.
     * Off by default: for a catalog whose version is not frozen the client
     * revalidates at every transaction start, so this would turn a cheap
     * {@code catalog_version} poll into a full build.
     */
    CONTENT_HASH
}
