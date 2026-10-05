// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.CatalogContentsResponse;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The default {@code catalog_contents} answer: the whole catalog, composed from
 * a service's own per-schema RPCs.
 *
 * <p>{@code catalog_contents} replaces {@code catalog_schemas} plus one
 * {@code catalog_schema_contents_*} call per schema and kind. Its contract is
 * that every item is byte-for-byte what the matching per-schema RPC returns,
 * so the composition calls exactly those methods on the same service and
 * copies their {@code items} through — there is no second serialiser to drift.
 * A kind whose {@code estimated_object_count} is exactly {@code 0} is not
 * asked for (the client treats that count as a guarantee and would skip the
 * RPC too); a missing count, or no counts at all, means the kind is listed.</p>
 *
 * <p>The answer is catalog-wide, not per transaction: the client caches it for
 * the whole attach, so every per-schema call is made with no transaction.
 * Schemas come back parents before children, as {@code catalog_schemas}
 * promises.</p>
 *
 * <p>Mirrors vgi-python's {@code CatalogInterface.catalog_contents} default. A
 * service that can build the snapshot more cheaply overrides
 * {@link VgiService#catalog_contents}.</p>
 */
public final class CatalogContents {

    private CatalogContents() {}

    /**
     * Compose the whole-catalog answer from {@code service}'s per-schema RPCs.
     *
     * @param service            the service whose per-schema RPCs are composed
     * @param attach_opaque_data the attach handle, passed through unchanged
     * @param ctx                the per-call context, passed through unchanged (may be null)
     * @return the catalog version and one serialised {@code SchemaContents} per schema
     */
    public static CatalogContentsResponse compose(VgiService service, byte[] attach_opaque_data,
                                                  @Nullable CallContext ctx) {
        long version = service.catalog_version(attach_opaque_data, null, ctx).version();
        List<SchemaContents> contents = schemaContents(service, attach_opaque_data, ctx);
        List<byte[]> schemas = new ArrayList<>(contents.size());
        for (SchemaContents c : contents) {
            schemas.add(RecordCodec.serializeToBytes(c));
        }
        return new CatalogContentsResponse(version, schemas);
    }

    /**
     * One {@link SchemaContents} per schema, parents before children.
     *
     * @param service            the service whose per-schema RPCs are composed
     * @param attach_opaque_data the attach handle
     * @param ctx                the per-call context (may be null)
     * @return the per-schema contents, in {@code catalog_schemas} order made parent-first
     */
    public static List<SchemaContents> schemaContents(VgiService service, byte[] attach_opaque_data,
                                                      @Nullable CallContext ctx) {
        record Entry(byte[] item, SchemaInfo info) {}
        List<Entry> entries = new ArrayList<>();
        for (byte[] item : service.catalog_schemas(attach_opaque_data, null).items()) {
            entries.add(new Entry(item, RecordCodec.deserializeFromBytes(item, SchemaInfo.class)));
        }
        // Stable: schemas at the same depth keep the order catalog_schemas gave them.
        entries.sort(Comparator.comparingInt(e -> e.info().path().size()));

        List<SchemaContents> out = new ArrayList<>(entries.size());
        for (Entry e : entries) {
            List<String> path = e.info().path();
            Map<String, Long> counts = e.info().estimated_object_count();
            out.add(new SchemaContents(
                    e.item(),
                    kind(counts, "table",
                            () -> service.catalog_schema_contents_tables(attach_opaque_data, path, null, ctx)),
                    kind(counts, "view",
                            () -> service.catalog_schema_contents_views(attach_opaque_data, path, null)),
                    kind(counts, "scalar_function", () -> service.catalog_schema_contents_functions(
                            attach_opaque_data, path, "SCALAR_FUNCTION", null, ctx)),
                    kind(counts, "aggregate_function", () -> service.catalog_schema_contents_functions(
                            attach_opaque_data, path, "AGGREGATE_FUNCTION", null, ctx)),
                    kind(counts, "table_function", () -> service.catalog_schema_contents_functions(
                            attach_opaque_data, path, "TABLE_FUNCTION", null, ctx)),
                    kind(counts, "macro", () -> service.catalog_schema_contents_macros(
                            attach_opaque_data, path, "SCALAR_MACRO", null)),
                    kind(counts, "macro", () -> service.catalog_schema_contents_macros(
                            attach_opaque_data, path, "TABLE_MACRO", null)),
                    kind(counts, "index",
                            () -> service.catalog_schema_contents_indexes(attach_opaque_data, path, null))));
        }
        return out;
    }

    private static List<byte[]> kind(@Nullable Map<String, Long> counts, String countKey,
                                     Supplier<ItemsResponse> list) {
        if (counts != null) {
            Long count = counts.get(countKey);
            if (count != null && count == 0L) return List.of();
        }
        return List.copyOf(list.get().items());
    }
}
