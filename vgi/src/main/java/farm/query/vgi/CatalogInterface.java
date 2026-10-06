// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.OnConflict;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableCreateRequest;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.ViewInfo;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A catalog whose metadata is computed by code rather than declared on the
 * {@link Worker}: the Java counterpart of vgi-python's {@code CatalogInterface}.
 * Register one with {@link Worker#registerCatalog(CatalogInterface)} and it is
 * served next to the worker's own catalog (MetaWorker-style): it gets its own
 * row in {@code catalog_catalogs()}, and every catalog RPC for one of its
 * attaches is routed here.
 *
 * <p>The framework owns the attach handle. At {@code catalog_attach} it mints
 * a random {@code attachId}, passes it to {@link #attach}, and seals it into
 * the {@code attach_opaque_data} the client keeps; every later call unseals it
 * (the auth check) and hands this interface the plain {@code attachId} again.
 * State that must be private to one ATTACH is therefore keyed by
 * {@code attachId}.</p>
 *
 * <p>Only the catalog plane is routed here. The catalog declares no functions
 * of its own, so a function bind on one of its attaches is refused.</p>
 *
 * <p>Every DDL method defaults to refusing ("read-only"). A DDL-capable catalog
 * overrides them and bumps its {@link #version} on every change, so the client
 * (which checks the version, or the {@code catalog_contents} etag, at every
 * transaction start) sees the change. {@link farm.query.vgi.catalog.InMemoryCatalog}
 * is a complete DDL-capable implementation.</p>
 */
public interface CatalogInterface {

    /**
     * The catalog name, as used in {@code ATTACH '<name>' ...}.
     *
     * @return the name
     */
    String name();

    /**
     * The advertised implementation version, or {@code null}.
     *
     * @return the implementation version
     */
    default @Nullable String implementationVersion() { return null; }

    /**
     * The advertised {@code data_version_spec}, or {@code null}.
     *
     * @return the data version spec
     */
    default @Nullable String dataVersionSpec() { return null; }

    /**
     * What {@link #attach} reports about the attached catalog.
     *
     * @param defaultSchema            the default schema name
     * @param comment                  the catalog comment, or {@code null}
     * @param tags                     catalog tags
     * @param versionFrozen            the catalog version never changes for this attach
     * @param supportsCatalogContents  advertise {@code catalog_contents}
     */
    record AttachInfo(String defaultSchema, @Nullable String comment, Map<String, String> tags,
                      boolean versionFrozen, boolean supportsCatalogContents) {

        /** Defensive copy; null tags read as none. */
        public AttachInfo {
            tags = tags == null ? Map.of() : Map.copyOf(tags);
        }
    }

    /**
     * Attach the catalog. Called once per {@code ATTACH}.
     *
     * @param attachId the framework-minted id for this attach (key private state on it)
     * @param request  the attach request (name, options, version specs)
     * @return the attach description
     */
    AttachInfo attach(byte[] attachId, CatalogAttachRequest request);

    /**
     * Release an attach. Default: nothing to release.
     *
     * @param attachId the attach id
     */
    default void detach(byte[] attachId) {}

    /**
     * The catalog version for this attach. {@code 0} means "unknown": the client
     * then reloads the catalog at every transaction start.
     *
     * @param attachId the attach id
     * @return the version
     */
    long version(byte[] attachId);

    /**
     * Every schema of the attached catalog, parents before children. The
     * {@code attach_opaque_data} of each returned {@link SchemaInfo} is replaced
     * by the framework, so any value will do.
     *
     * @param attachId the attach id
     * @return the schemas
     */
    List<SchemaInfo> schemas(byte[] attachId);

    /**
     * One schema by path. Default: searched in {@link #schemas}.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @return the schema, or empty
     */
    default Optional<SchemaInfo> schema(byte[] attachId, List<String> path) {
        for (SchemaInfo s : schemas(attachId)) {
            if (samePath(s.path(), path)) return Optional.of(s);
        }
        return Optional.empty();
    }

    /**
     * The tables of a schema. Default: none.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @return the tables
     */
    default List<TableInfo> tables(byte[] attachId, List<String> path) { return List.of(); }

    /**
     * The views of a schema. Default: none.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @return the views
     */
    default List<ViewInfo> views(byte[] attachId, List<String> path) { return List.of(); }

    /**
     * The macros of a schema of one type. Default: none.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @param type     the macro type
     * @return the macros
     */
    default List<MacroInfo> macros(byte[] attachId, List<String> path, MacroType type) { return List.of(); }

    /**
     * One table by name. Default: searched in {@link #tables}.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @param name     the table name
     * @return the table, or empty
     */
    default Optional<TableInfo> table(byte[] attachId, List<String> path, String name) {
        return tables(attachId, path).stream().filter(t -> t.name().equals(name)).findFirst();
    }

    /**
     * One view by name. Default: searched in {@link #views}.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @param name     the view name
     * @return the view, or empty
     */
    default Optional<ViewInfo> view(byte[] attachId, List<String> path, String name) {
        return views(attachId, path).stream().filter(v -> v.name().equals(name)).findFirst();
    }

    /**
     * One macro by name, of either type. Default: searched in {@link #macros}.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @param name     the macro name
     * @return the macro, or empty
     */
    default Optional<MacroInfo> macro(byte[] attachId, List<String> path, String name) {
        for (MacroType type : MacroType.values()) {
            for (MacroInfo m : macros(attachId, path, type)) {
                if (m.name().equals(name)) return Optional.of(m);
            }
        }
        return Optional.empty();
    }

    // ------------------------------------------------------------------ DDL

    /**
     * {@code CREATE SCHEMA}. Default: read-only.
     *
     * @param attachId   the attach id
     * @param path       the schema path
     * @param onConflict what to do when it exists
     * @param comment    the schema comment, or {@code null}
     * @param tags       schema tags
     */
    default void schemaCreate(byte[] attachId, List<String> path, OnConflict onConflict,
                              @Nullable String comment, Map<String, String> tags) {
        throw readOnly("catalog_schema_create");
    }

    /**
     * {@code DROP SCHEMA}. Default: read-only.
     *
     * @param attachId       the attach id
     * @param path           the schema path
     * @param ignoreNotFound {@code IF EXISTS}
     * @param cascade        also drop the schema's objects
     */
    default void schemaDrop(byte[] attachId, List<String> path, boolean ignoreNotFound, boolean cascade) {
        throw readOnly("catalog_schema_drop");
    }

    /**
     * {@code CREATE TABLE}. Default: read-only.
     *
     * @param attachId the attach id
     * @param request  the create request (columns, constraints, on_conflict)
     */
    default void tableCreate(byte[] attachId, TableCreateRequest request) {
        throw readOnly("catalog_table_create");
    }

    /**
     * {@code DROP TABLE}. Default: read-only.
     *
     * @param attachId       the attach id
     * @param path           the schema path
     * @param name           the table name
     * @param ignoreNotFound {@code IF EXISTS}
     * @param cascade        also drop dependents
     */
    default void tableDrop(byte[] attachId, List<String> path, String name, boolean ignoreNotFound,
                           boolean cascade) {
        throw readOnly("catalog_table_drop");
    }

    /**
     * {@code CREATE VIEW}. Default: read-only.
     *
     * @param attachId   the attach id
     * @param path       the schema path
     * @param name       the view name
     * @param definition the view's SQL
     * @param onConflict what to do when it exists
     */
    default void viewCreate(byte[] attachId, List<String> path, String name, String definition,
                            OnConflict onConflict) {
        throw readOnly("catalog_view_create");
    }

    /**
     * {@code DROP VIEW}. Default: read-only.
     *
     * @param attachId       the attach id
     * @param path           the schema path
     * @param name           the view name
     * @param ignoreNotFound {@code IF EXISTS}
     * @param cascade        also drop dependents
     */
    default void viewDrop(byte[] attachId, List<String> path, String name, boolean ignoreNotFound,
                          boolean cascade) {
        throw readOnly("catalog_view_drop");
    }

    // ------------------------------------------------------- catalog_contents

    /**
     * Answer {@code catalog_contents} (only called when {@link AttachInfo#supportsCatalogContents}).
     * The request's {@link CatalogContentsProvider.Request#build()} is the
     * default snapshot (this catalog's per-schema listings composed); override
     * to answer {@code not_modified} from a cheap validator or to fail.
     * Default: the default snapshot, no etag.
     *
     * @param request the call
     * @return the answer, checked by {@link CatalogContents#respond}
     */
    default CatalogContentsResult catalogContents(CatalogContentsProvider.Request request) {
        return CatalogContentsResult.of(request.build());
    }

    /**
     * The framework etag policy for this catalog's {@code catalog_contents}.
     * Default: {@link CatalogContentsEtag#NONE}.
     *
     * @return the policy
     */
    default CatalogContentsEtag catalogContentsEtag() { return CatalogContentsEtag.NONE; }

    /**
     * The exception every refused DDL throws.
     *
     * @param method the RPC refused
     * @return the exception
     */
    private UnsupportedOperationException readOnly(String method) {
        return new UnsupportedOperationException(
                "catalog " + name() + " is read-only: " + method + " not supported");
    }

    /**
     * Schema paths compare case-insensitively, as DuckDB identifiers do.
     *
     * @param a one path
     * @param b the other
     * @return whether they name the same schema
     */
    static boolean samePath(List<String> a, List<String> b) {
        if (a == null || b == null || a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).equalsIgnoreCase(b.get(i))) return false;
        }
        return true;
    }
}
