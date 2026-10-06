// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * What a catalog answers to {@code catalog_contents}: a snapshot of every
 * schema, or "not modified".
 *
 * <p>The Java counterpart of vgi-python's {@code CatalogContentsResult}. A
 * {@link CatalogContentsProvider} returns one; the framework
 * ({@link CatalogContents#respond}) checks it and turns it into the wire
 * {@code CatalogContentsResponse}.</p>
 *
 * @param schemas     one {@link SchemaContents} per schema; must be empty when
 *                    {@code notModified}. Any order: the framework sends parents first.
 * @param etag        opaque validator for this snapshot (a generation counter, a
 *                    schema version, a git sha, ...) that the client sends back as
 *                    {@code if_none_match}; {@code null} means the catalog does not
 *                    revalidate (unless the worker opts in to
 *                    {@link CatalogContentsEtag#CONTENT_HASH})
 * @param notModified the request's {@code if_none_match} equals the current etag,
 *                    so the catalog skipped building the snapshot. Requires that
 *                    matching {@code etag}.
 */
public record CatalogContentsResult(List<SchemaContents> schemas, @Nullable String etag, boolean notModified) {

    /** Defensive copy; a {@code null} schema list reads as none. */
    public CatalogContentsResult {
        schemas = schemas == null ? List.of() : List.copyOf(schemas);
    }

    /**
     * A full snapshot with no etag: the catalog does not revalidate.
     *
     * @param schemas one entry per schema
     * @return the result
     */
    public static CatalogContentsResult of(List<SchemaContents> schemas) {
        return new CatalogContentsResult(schemas, null, false);
    }

    /**
     * A full snapshot carrying its etag.
     *
     * @param schemas one entry per schema
     * @param etag    the snapshot's validator
     * @return the result
     */
    public static CatalogContentsResult of(List<SchemaContents> schemas, String etag) {
        return new CatalogContentsResult(schemas, Objects.requireNonNull(etag, "etag"), false);
    }

    /**
     * "The client's snapshot is current": answered when {@code if_none_match}
     * equals {@code etag}, before building anything.
     *
     * @param etag the current etag (equal to the request's {@code if_none_match})
     * @return the result
     */
    public static CatalogContentsResult notModified(String etag) {
        return new CatalogContentsResult(List.of(), Objects.requireNonNull(etag, "etag"), true);
    }
}
