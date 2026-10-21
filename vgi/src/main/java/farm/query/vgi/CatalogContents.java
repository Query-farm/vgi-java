// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.CatalogContentsResponse;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.ViewInfo;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgi.protocol.IndexInfo;
import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The {@code catalog_contents} machinery: the default snapshot composed from a
 * service's own per-schema RPCs, and the framework rules every answer goes
 * through (revalidation, path checks, parent-first order, the content-hash etag).
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
 * <p>Mirrors vgi-python's {@code CatalogInterface.catalog_contents} default and
 * {@code Worker.catalog_contents}. A catalog that can build the snapshot more
 * cheaply, or revalidate with an etag, installs a {@link CatalogContentsProvider}.</p>
 */
public final class CatalogContents {

    private CatalogContents() {}

    /**
     * Answer {@code catalog_contents} with the default snapshot and no etag.
     *
     * @param service            the service whose per-schema RPCs are composed
     * @param attach_opaque_data the attach handle, passed through unchanged
     * @param ctx                the per-call context, passed through unchanged (may be null)
     * @return the catalog version and one {@code SchemaContents} per schema
     */
    public static CatalogContentsResponse compose(VgiService service, byte[] attach_opaque_data,
                                                  @Nullable CallContext ctx) {
        return serve(service, attach_opaque_data, null, ctx, "", null, CatalogContentsEtag.NONE);
    }

    /**
     * Answer one {@code catalog_contents} call: ask the provider (or build the
     * default snapshot), then apply {@link #respond}.
     *
     * @param service            the service whose per-schema RPCs build the default snapshot
     * @param attach_opaque_data the attach handle, passed through unchanged
     * @param if_none_match      the client's etag, or {@code null}
     * @param ctx                the per-call context (may be null)
     * @param catalogName        the attached catalog's name, for the provider
     * @param provider           the catalog's own answer, or {@code null} for the default
     * @param etagMode           the framework etag policy
     * @return the wire response
     */
    public static CatalogContentsResponse serve(VgiService service, byte[] attach_opaque_data,
                                                @Nullable String if_none_match, @Nullable CallContext ctx,
                                                String catalogName, @Nullable CatalogContentsProvider provider,
                                                CatalogContentsEtag etagMode) {
        long version = service.catalog_version(attach_opaque_data, null, ctx).version();
        Supplier<List<SchemaContents>> build = () -> schemaContents(service, attach_opaque_data, ctx);
        CatalogContentsResult result = provider == null
                ? CatalogContentsResult.of(build.get())
                : provider.catalogContents(new CatalogContentsProvider.Request(
                        catalogName, attach_opaque_data, version, if_none_match, ctx, build));
        if (result == null) {
            throw new IllegalStateException("catalog_contents provider returned null");
        }
        return respond(version, result, if_none_match, etagMode);
    }

    /**
     * Shape a catalog's answer into the wire response, enforcing the rules.
     *
     * <ul>
     *   <li>{@code not_modified} needs an etag equal to {@code if_none_match}
     *       and no schemas; a catalog with no etag never yields it.</li>
     *   <li>Each schema's {@code path} must equal the {@code SchemaInfo.path}
     *       inside it; paths are unique and every parent is present. Schemas
     *       are sent parents first (stable within a depth).</li>
     *   <li>With {@link CatalogContentsEtag#CONTENT_HASH} and no etag of the
     *       catalog's own, the etag is {@link #digest} of the snapshot.</li>
     *   <li>A full answer whose etag equals {@code if_none_match} is sent as
     *       {@code not_modified}. With no etag, {@code if_none_match} is ignored.</li>
     * </ul>
     *
     * @param version       the catalog version the answer is for
     * @param result        the catalog's answer
     * @param if_none_match the client's etag, or {@code null}
     * @param etagMode      the framework etag policy
     * @return the wire response
     * @throws IllegalStateException if the answer breaks a rule
     */
    public static CatalogContentsResponse respond(long version, CatalogContentsResult result,
                                                  @Nullable String if_none_match, CatalogContentsEtag etagMode) {
        if (result.notModified()) {
            if (result.etag() == null || if_none_match == null || !result.etag().equals(if_none_match)) {
                throw new IllegalStateException("catalog_contents returned not_modified, but only a catalog "
                        + "whose etag equals if_none_match may (and it must return that etag)");
            }
            if (!result.schemas().isEmpty()) {
                throw new IllegalStateException(
                        "catalog_contents returned not_modified with schemas; it must return none");
            }
            return unchanged(version, result.etag());
        }
        List<SchemaContents> schemas = parentsFirst(result.schemas());
        String etag = result.etag();
        if (etag != null && etag.equals(if_none_match)) return unchanged(version, etag);
        CatalogContentsResponse response = new CatalogContentsResponse(version, etag, false,
                MetadataCollections.merge(schemas.stream().map(SchemaContents::schema).toList(), SchemaInfo.class),
                MetadataCollections.merge(schemas.stream().flatMap(c -> c.tables().stream()).toList(), TableInfo.class),
                MetadataCollections.merge(schemas.stream().flatMap(c -> c.views().stream()).toList(), ViewInfo.class),
                MetadataCollections.merge(schemas.stream().flatMap(c -> java.util.stream.Stream.of(c.scalar_functions(), c.aggregate_functions(), c.table_functions()).flatMap(List::stream)).toList(), FunctionInfo.class),
                MetadataCollections.merge(schemas.stream().flatMap(c -> java.util.stream.Stream.of(c.scalar_macros(), c.table_macros()).flatMap(List::stream)).toList(), MacroInfo.class),
                MetadataCollections.merge(schemas.stream().flatMap(c -> c.indexes().stream()).toList(), IndexInfo.class));
        if (etag == null && etagMode == CatalogContentsEtag.CONTENT_HASH) {
            etag = digest(response);
            response = new CatalogContentsResponse(version, etag, false, response.schemas(), response.tables(), response.views(), response.functions(), response.macros(), response.indexes());
        }
        return etag != null && etag.equals(if_none_match) ? unchanged(version, etag) : response;

    }

    public static String digest(CatalogContentsResponse response) { return farm.query.vgi.catalog.snapshot.CatalogSnapshotManager.digest(response); }

    public static CatalogContentsResponse unchanged(long version, String etag) {
        return new CatalogContentsResponse(version, etag, true, null, null, null, null, null, null);
    }

    /** Decode the collection streams into the existing schema-grouped author shape. */
    public static List<SchemaContents> schemas(CatalogContentsResponse response) {
        if (response.not_modified()) {
            if (response.etag() == null || response.schemas() != null || response.tables() != null
                    || response.views() != null || response.functions() != null || response.macros() != null || response.indexes() != null)
                throw new IllegalStateException("not_modified requires an ETag and six null streams");
            return List.of();
        }
        Map<List<String>, SchemaContents> byPath = new java.util.LinkedHashMap<>();
        for (SchemaInfo info : MetadataCollections.decode(response.schemas(), SchemaInfo.class)) {
            List<String> path = List.copyOf(info.path());
            if (path.isEmpty() || path.stream().anyMatch(String::isEmpty) || byPath.containsKey(path)) throw new IllegalStateException("empty or duplicate schema path");
            if (path.size() > 1 && !byPath.containsKey(path.subList(0, path.size() - 1)))
                throw new IllegalStateException("schema parent missing or out of order");
            byPath.put(path, new SchemaContents(path, RecordCodec.serializeToBytes(info), new ArrayList<>(), new ArrayList<>(),
                    new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>()));
        }
        java.util.function.Function<List<String>, SchemaContents> owner = path -> {
            SchemaContents schema = byPath.get(path);
            if (schema == null) throw new IllegalStateException("catalog object refers to missing schema " + path);
            return schema;
        };
        for (TableInfo row : MetadataCollections.decode(response.tables(), TableInfo.class)) owner.apply(row.schema_path()).tables().add(RecordCodec.serializeToBytes(row));
        for (ViewInfo row : MetadataCollections.decode(response.views(), ViewInfo.class)) owner.apply(row.schema_path()).views().add(RecordCodec.serializeToBytes(row));
        for (FunctionInfo row : MetadataCollections.decode(response.functions(), FunctionInfo.class)) {
            SchemaContents schema = owner.apply(row.schema_path());
            List<byte[]> items = switch (row.function_type()) {
                case "SCALAR", "scalar" -> schema.scalar_functions();
                case "AGGREGATE", "aggregate" -> schema.aggregate_functions();
                case "TABLE", "TABLE_BUFFERING", "table", "table_buffering", "table_in_out" -> schema.table_functions();
                default -> throw new IllegalStateException("unknown function type " + row.function_type());
            };
            items.add(RecordCodec.serializeToBytes(row));
        }
        for (MacroInfo row : MetadataCollections.decode(response.macros(), MacroInfo.class)) {
            SchemaContents schema = owner.apply(row.schema_path());
            List<byte[]> items = switch (row.macro_type()) {
                case "SCALAR", "scalar" -> schema.scalar_macros();
                case "TABLE", "table" -> schema.table_macros();
                default -> throw new IllegalStateException("unknown macro type " + row.macro_type());
            };
            items.add(RecordCodec.serializeToBytes(row));
        }
        for (IndexInfo row : MetadataCollections.decode(response.indexes(), IndexInfo.class)) owner.apply(row.schema_path()).indexes().add(RecordCodec.serializeToBytes(row));
        return new ArrayList<>(byPath.values());
    }

    /**
     * Validate the schema paths and order the schemas parents first.
     *
     * @param schemas the snapshot, in any order
     * @return the same entries, parents before children (stable within a depth)
     * @throws IllegalStateException on an empty, duplicate, orphaned or mismatched path
     */
    static List<SchemaContents> parentsFirst(List<SchemaContents> schemas) {
        Set<List<String>> paths = new HashSet<>();
        for (SchemaContents c : schemas) {
            List<String> path = c.path();
            if (path == null || path.isEmpty()) {
                throw new IllegalStateException("catalog_contents returned a schema with an empty path");
            }
            List<String> inner = RecordCodec.deserializeFromBytes(c.schema(), SchemaInfo.class).path();
            if (!path.equals(inner)) {
                throw new IllegalStateException("catalog_contents returned schema path " + path
                        + " for a SchemaInfo whose path is " + inner);
            }
            if (!paths.add(List.copyOf(path))) {
                throw new IllegalStateException("catalog_contents returned duplicate schema path " + path);
            }
        }
        for (List<String> path : paths) {
            if (path.size() > 1 && !paths.contains(path.subList(0, path.size() - 1))) {
                throw new IllegalStateException(
                        "catalog_contents returned schema path " + path + " without its parent");
            }
        }
        List<SchemaContents> ordered = new ArrayList<>(schemas);
        ordered.sort(Comparator.comparingInt(c -> c.path().size()));
        return ordered;
    }

    /**
     * Hex SHA-256 over a {@code catalog_contents} snapshot: the
     * {@link CatalogContentsEtag#CONTENT_HASH} etag.
     *
     * <p>Byte-compatible with vgi-python's {@code catalog_contents_digest}: the
     * schema count, then per schema its path parts, its {@code SchemaInfo}
     * item, and each kind's items in wire order, every sequence prefixed by its
     * length and every byte string by its size (8-byte little-endian), so no two
     * different snapshots share an input. Deterministic because item encoding
     * is (map columns are written in key order), so two builds of the same
     * catalog hash alike.</p>
     *
     * @param schemas the snapshot, in wire order
     * @return the lowercase hex digest
     */
    public static String digest(List<SchemaContents> schemas) {
        MessageDigest sha;
        try {
            sha = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
        length(sha, schemas.size());
        for (SchemaContents c : schemas) {
            List<byte[]> parts = new ArrayList<>(c.path().size());
            for (String part : c.path()) parts.add(part.getBytes(StandardCharsets.UTF_8));
            chunks(sha, parts);
            chunk(sha, c.schema());
            chunks(sha, c.tables());
            chunks(sha, c.views());
            chunks(sha, c.scalar_functions());
            chunks(sha, c.aggregate_functions());
            chunks(sha, c.table_functions());
            chunks(sha, c.scalar_macros());
            chunks(sha, c.table_macros());
            chunks(sha, c.indexes());
        }
        return HexFormat.of().formatHex(sha.digest());
    }

    private static void length(MessageDigest sha, long n) {
        sha.update(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(n).array());
    }

    private static void chunk(MessageDigest sha, byte[] data) {
        length(sha, data.length);
        sha.update(data);
    }

    private static void chunks(MessageDigest sha, List<byte[]> values) {
        length(sha, values.size());
        for (byte[] v : values) chunk(sha, v);
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
        for (byte[] item : service.catalog_schemas(attach_opaque_data, null, ctx).items()) {
            entries.add(new Entry(item, RecordCodec.deserializeFromBytes(item, SchemaInfo.class)));
        }
        // Stable: schemas at the same depth keep the order catalog_schemas gave them.
        entries.sort(Comparator.comparingInt(e -> e.info().path().size()));

        List<SchemaContents> out = new ArrayList<>(entries.size());
        for (Entry e : entries) {
            List<String> path = e.info().path();
            Map<String, Long> counts = e.info().estimated_object_count();
            out.add(new SchemaContents(
                    List.copyOf(path),
                    e.item(),
                    kind(counts, "table",
                            () -> service.catalog_schema_contents_tables(attach_opaque_data, path, null, ctx)),
                    kind(counts, "view",
                            () -> service.catalog_schema_contents_views(attach_opaque_data, path, null, ctx)),
                    kind(counts, "scalar_function", () -> service.catalog_schema_contents_functions(
                            attach_opaque_data, path, "SCALAR_FUNCTION", null, ctx)),
                    kind(counts, "aggregate_function", () -> service.catalog_schema_contents_functions(
                            attach_opaque_data, path, "AGGREGATE_FUNCTION", null, ctx)),
                    kind(counts, "table_function", () -> service.catalog_schema_contents_functions(
                            attach_opaque_data, path, "TABLE_FUNCTION", null, ctx)),
                    kind(counts, "macro", () -> service.catalog_schema_contents_macros(
                            attach_opaque_data, path, "SCALAR_MACRO", null, ctx)),
                    kind(counts, "macro", () -> service.catalog_schema_contents_macros(
                            attach_opaque_data, path, "TABLE_MACRO", null, ctx)),
                    kind(counts, "index",
                            () -> service.catalog_schema_contents_indexes(attach_opaque_data, path, null, ctx))));
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
