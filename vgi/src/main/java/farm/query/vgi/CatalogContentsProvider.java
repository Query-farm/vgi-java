// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.function.Supplier;

/**
 * A catalog's own answer to {@code catalog_contents}, installed with
 * {@link Worker#catalogContents(CatalogContentsProvider)}.
 *
 * <p>The provider sees the client's {@code if_none_match}, so a cheap
 * validator (a generation counter, a schema version, a git sha) can answer
 * {@link CatalogContentsResult#notModified} <em>before</em> building anything,
 * and otherwise return the snapshot with its etag. The default snapshot (the
 * per-schema RPCs composed, as without a provider) is one call away:
 * {@link Request#build()}.</p>
 *
 * <p>Mirrors overriding {@code CatalogInterface.catalog_contents} in
 * vgi-python. Whatever it returns is checked by {@link CatalogContents#respond}.</p>
 */
@FunctionalInterface
public interface CatalogContentsProvider {

    /**
     * Answer one {@code catalog_contents} call.
     *
     * @param request the call: catalog, version, {@code if_none_match}, and the default builder
     * @return the snapshot with its etag, or {@code not_modified}
     */
    CatalogContentsResult catalogContents(Request request);

    /**
     * One {@code catalog_contents} call, as a provider sees it.
     *
     * @param catalogName       the attached catalog's name (the worker's own, or an extra catalog's)
     * @param attachOpaqueData  the attach handle as the client sent it
     * @param catalogVersion    the catalog version the answer is for
     * @param ifNoneMatch       the etag of the snapshot the client holds, or {@code null}
     * @param ctx               the per-call context (may be {@code null})
     * @param defaultContents   builds the default snapshot (one entry per schema)
     */
    record Request(String catalogName, byte[] attachOpaqueData, long catalogVersion,
                   @Nullable String ifNoneMatch, @Nullable CallContext ctx,
                   Supplier<List<SchemaContents>> defaultContents) {

        /**
         * Build the default snapshot: the catalog's per-schema RPCs composed.
         *
         * @return one entry per schema
         */
        public List<SchemaContents> build() {
            return defaultContents.get();
        }
    }

    /**
     * A provider whose etag is a cheap validator: {@code "gen-<n>"}, where
     * {@code n} is the catalog version. It answers a matching
     * {@code if_none_match} with {@code not_modified} without building, and
     * otherwise builds the default snapshot. Right for a catalog whose version
     * changes whenever its contents do (the analogue of vgi-python's
     * {@code contents_reval} fixture).
     *
     * @return the provider
     */
    static CatalogContentsProvider versionEtag() {
        return request -> {
            String etag = "gen-" + request.catalogVersion();
            if (etag.equals(request.ifNoneMatch())) return CatalogContentsResult.notModified(etag);
            return CatalogContentsResult.of(request.build(), etag);
        };
    }
}
