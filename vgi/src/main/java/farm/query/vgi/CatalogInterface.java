// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.OnConflict;
import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.FunctionInfo;
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
 * <p>Only the catalog plane is routed here. A function bind on one of its
 * attaches reaches only functions registered on the worker into this catalog
 * ({@code Worker#registerExtraCatalog*} with {@link #name()}), and the catalog
 * lists them itself through {@link #functions} (protocol 3.3: every function a
 * table names must be listed; see there). By default it lists none, and every
 * bind is refused.</p>
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
     * @param defaultSchema            one raw root identifier, or null for no implicit default
     * @param comment                  the catalog comment, or {@code null}
     * @param tags                     catalog tags
     * @param versionFrozen            the catalog version never changes for this attach
     * @param supportsCatalogContents  advertise {@code catalog_contents}
     */
    record AttachInfo(@Nullable String defaultSchema, @Nullable String comment, Map<String, String> tags,
                      boolean versionFrozen, boolean supportsCatalogContents) {

        /** Defensive copy; null tags read as none. */
        public AttachInfo {
            if (defaultSchema != null && (defaultSchema.isEmpty() || defaultSchema.indexOf('\0') >= 0)) {
                throw new IllegalArgumentException("defaultSchema must be a non-empty raw root identifier without NUL, or null");
            }
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
     * The read profiles this catalog implements for an attach, advertised on the attach result
     * and selected against the client's offer by the framework. Tables carrying canonical
     * constraint records ({@link farm.query.vgi.catalog.ConstraintRecord}) are readable only
     * under {@link farm.query.vgi.catalog.ConstraintRecord#CONSTRAINT_RECORDS_READ}.
     *
     * <p>Default: both constraint read profiles when any table of {@link #schemas} carries
     * records, found by listing every schema's tables right after {@link #attach}. Override
     * when that listing is expensive, or when tables gain records after attach.
     *
     * @param attachId the attach id
     * @return the read profiles, or empty
     */
    default List<String> catalogReadProfiles(byte[] attachId) {
        for (SchemaInfo s : schemas(attachId)) {
            for (TableInfo t : tables(attachId, s.path())) {
                if (!t.constraints().isEmpty()) {
                    return List.of(farm.query.vgi.catalog.ConstraintRecord.CONSTRAINT_RECORDS_READ,
                            farm.query.vgi.catalog.ConstraintRecord.SCALAR_EXPRESSION_READ);
                }
            }
        }
        return List.of();
    }

    /**
     * This catalog's own capability advertisement and selection for an attach: build it with
     * {@link CatalogProfiles#negotiate}. The framework merges in {@link #catalogReadProfiles},
     * and gates the DDL it forwards on the selection: {@code CREATE TABLE} with constraint
     * definitions reaches {@link #tableCreate} only when {@link CatalogProfiles#CONSTRAINT_CREATE},
     * {@link CatalogProfiles#CONSTRAINT_ENFORCE} and {@link CatalogProfiles#CONSTRAINT_RECORDS_READ}
     * were selected (DuckDB sends none otherwise). A catalog that selects them enforces every
     * record it then serves, on every write path.
     *
     * <p>Default: no decision of its own.
     *
     * @param attachId the attach id
     * @param request  the attach request (its {@code client_capabilities} is the client's offer)
     * @return the decision; either side may be null
     */
    default Worker.CatalogCapabilitySelection catalogCapabilities(byte[] attachId, CatalogAttachRequest request) {
        return new Worker.CatalogCapabilitySelection(null, null);
    }

    /**
     * Release an attach. Default: nothing to release.
     *
     * @param attachId the attach id
     */
    default void detach(byte[] attachId) {}

    /**
     * Whether this attach's result advertises {@code supports_transactions}. When true, DuckDB
     * calls {@link #transactionBegin} / {@link #transactionCommit} / {@link #transactionRollback}
     * for every transaction on the attach (autocommit statements included). Default false.
     *
     * @param attachId the attach id
     * @return whether the attach supports transactions
     */
    default boolean supportsTransactions(byte[] attachId) { return false; }

    /**
     * Mint a transaction's plaintext id ({@code catalog_transaction_begin}); the framework seals
     * it on HTTP and hands the commit / rollback hooks the same bytes. Default: 16 random bytes.
     *
     * @param attachId the attach id
     * @return the transaction id, or {@code null} for none
     */
    default byte[] transactionBegin(byte[] attachId) {
        byte[] id = new byte[16];
        new java.security.SecureRandom().nextBytes(id);
        return id;
    }

    /**
     * Commit a transaction. An exception fails the client's {@code COMMIT}. Default: nothing.
     *
     * @param attachId      the attach id
     * @param transactionId the id {@link #transactionBegin} returned
     */
    default void transactionCommit(byte[] attachId, byte[] transactionId) {}

    /**
     * Roll a transaction back. Default: nothing.
     *
     * @param attachId      the attach id
     * @param transactionId the id {@link #transactionBegin} returned
     */
    default void transactionRollback(byte[] attachId, byte[] transactionId) {}

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
     * The macros of a schema of one type. Default: none. Build each family from a simple
     * descriptor with {@link MacroInfo#of(farm.query.vgi.catalog.Macro)}.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @param type     the macro type
     * @return the macros
     */
    default List<MacroInfo> macros(byte[] attachId, List<String> path, MacroType type) { return List.of(); }

    /**
     * The functions a schema lists ({@code catalog_schema_contents_functions}). Default: none.
     *
     * <p>Protocol 3.3: the extension takes a table's scan and write capabilities (projection and
     * filter pushdown, required secrets, {@code has_finalize}, ...) only from the function
     * listing, and refuses a table whose scan or write function no listing carries (DuckDB
     * built-ins such as {@code read_parquet} excepted). A catalog whose {@link TableInfo}s name
     * functions of this worker therefore lists each of them here, built with
     * {@link #functionInfoFor}:
     * <ul>
     *   <li>a table's insert / update / delete function with
     *       {@link FunctionInfo#VISIBILITY_TABLE_ONLY};</li>
     *   <li>a table's scan with {@link FunctionInfo#VISIBILITY_TABLE_ONLY}, unless SQL should be
     *       able to call it directly ({@link FunctionInfo#VISIBILITY_PUBLIC});</li>
     *   <li>any other function with {@link FunctionInfo#VISIBILITY_PUBLIC}.</li>
     * </ul>
     * A {@code table_only} function is absent from {@code duckdb_functions()} and not callable from
     * SQL; the extension uses it only to resolve the tables that name it.
     *
     * <p>Register the implementations on the worker into this catalog, so a bind on this
     * catalog's attach reaches them: {@link Worker#registerExtraCatalogTableFunction},
     * {@link Worker#registerExtraCatalogTableInOut}, {@link Worker#registerExtraCatalogScalar} or
     * {@link Worker#registerExtraCatalogAggregate} with {@link #name()} as the catalog. Report the
     * counts in the schema's {@link SchemaInfo} ({@code estimated_object_count}: {@code scalar_function},
     * {@code table_function}, {@code aggregate_function}): a zero count makes the
     * client skip this listing.
     *
     * @param attachId the attach id
     * @param path     the schema path
     * @return the functions listed in the schema
     */
    default List<FunctionInfo> functions(byte[] attachId, List<String> path) { return List.of(); }

    /**
     * The listing entry the framework would send for {@code fn} in {@code schemaPath}, with the
     * given visibility: the same capabilities (pushdown flags, required secrets,
     * {@code has_finalize}, arguments, output schema, ...) a function registered on the worker's
     * own catalog advertises. For {@link #functions}.
     *
     * @param fn         a scalar, table, table in-out, table-buffering or aggregate function
     * @param schemaPath the schema it is listed in
     * @param visibility {@link FunctionInfo#VISIBILITY_PUBLIC}, {@link FunctionInfo#VISIBILITY_TABLE_ONLY},
     *                   or {@code null} (public)
     * @return the entry
     * @throws IllegalArgumentException for an unknown visibility or function kind
     */
    static FunctionInfo functionInfoFor(FunctionDescriptor fn, List<String> schemaPath, @Nullable String visibility) {
        if (visibility != null && !FunctionInfo.VISIBILITY_PUBLIC.equals(visibility)
                && !FunctionInfo.VISIBILITY_TABLE_ONLY.equals(visibility)) {
            throw new IllegalArgumentException("functionInfoFor: visibility must be 'public' or 'table_only', not '"
                    + visibility + "'");
        }
        return farm.query.vgi.internal.VgiServiceImpl.describeFunction(fn, schemaPath).withVisibility(visibility);
    }

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
     * <p>Protocol 3.3 answers {@code catalog_table_create} with {@code created}, which the
     * framework takes from {@link #tableCreateReportingCreated}. A catalog that overrides only this
     * method keeps working: that default derives {@code created} from {@link #table} before
     * calling it. Override {@link #tableCreateReportingCreated} instead when the existence check
     * and the create must be one atomic step.
     *
     * @param attachId the attach id
     * @param request  the create request (columns, constraints, on_conflict)
     */
    default void tableCreate(byte[] attachId, TableCreateRequest request) {
        throw readOnly("catalog_table_create");
    }

    /**
     * {@code CREATE TABLE}, reporting whether this call created the table: the protocol 3.3
     * {@code catalog_table_create} result {@code created}.
     *
     * <p>Return {@code true} when this call created the table (for {@code OR REPLACE}, also when
     * it replaced an existing one), {@code false} when {@code IF NOT EXISTS}
     * ({@code on_conflict = "ignore"}) found an existing table and left it alone. The extension
     * relies on it: {@code CREATE TABLE IF NOT EXISTS t AS SELECT ...} inserts the query's rows
     * only when {@code created} is true, and drops the table again if a non-transactional CTAS
     * that created it then fails.
     *
     * <p>Default: for {@code IF NOT EXISTS}, {@code false} when {@link #table} already finds the
     * table (without calling {@link #tableCreate}); otherwise {@link #tableCreate} and
     * {@code true}.
     *
     * @param attachId the attach id
     * @param request  the create request (columns, constraints, on_conflict)
     * @return whether this call created the table
     */
    default boolean tableCreateReportingCreated(byte[] attachId, TableCreateRequest request) {
        if (OnConflict.fromWire(request.on_conflict()) == OnConflict.IGNORE
                && table(attachId, request.schema_path(), request.name()).isPresent()) {
            return false;
        }
        tableCreate(attachId, request);
        return true;
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
     * {@code CREATE VIEW}, as vgi 0.44 declared it. Default: read-only.
     *
     * @param attachId   the attach id
     * @param path       the schema path
     * @param name       the view name
     * @param definition the view's SQL
     * @param onConflict what to do when it exists
     * @deprecated override {@link #viewCreate(byte[], List, String, String, OnConflict, List)}
     *             (column aliases) or the full form (comment, tags, column comments); an
     *             override of this one is still reached for a view that uses none of them.
     */
    @Deprecated
    default void viewCreate(byte[] attachId, List<String> path, String name, String definition,
                            OnConflict onConflict) {
        throw readOnly("catalog_view_create");
    }

    /**
     * {@code CREATE VIEW} with optional column aliases. Default: the 5-argument form when there
     * are no aliases; with aliases, refused as read-only (or, when only the 5-argument form is
     * overridden, as unsupported, rather than dropping them).
     *
     * @param attach     the attach id
     * @param path       the schema path
     * @param name       the view name
     * @param definition the view's SQL
     * @param conflict   what to do when it exists
     * @param aliases    optional view column aliases
     */
    @SuppressWarnings("deprecation")
    default void viewCreate(byte[] attach, List<String> path, String name, String definition, OnConflict conflict,
                            List<String> aliases) {
        if (aliases == null || aliases.isEmpty()) {
            viewCreate(attach, path, name, definition, conflict);
            return;
        }
        if (overrides("viewCreate", byte[].class, List.class, String.class, String.class, OnConflict.class)) {
            throw new UnsupportedOperationException("catalog " + name() + " does not support view column aliases:"
                    + " override viewCreate(attachId, path, name, definition, onConflict, aliases)");
        }
        throw readOnly("catalog_view_create");
    }

    /**
     * {@code CREATE VIEW} with every option; the framework calls this one. Default: the
     * 6-argument form when there is no comment, tag or column comment; otherwise refused as
     * read-only (or, when a shorter form is overridden, as unsupported, rather than dropping them).
     *
     * @param attachId       the attach id
     * @param path           the schema path
     * @param name           the view name
     * @param definition     the view's SQL
     * @param onConflict     what to do when it exists
     * @param aliases        optional view column aliases
     * @param comment        the view comment, or {@code null}
     * @param tags           view tags, or {@code null}
     * @param columnComments per-output-column comments, or {@code null}
     */
    default void viewCreate(byte[] attachId, List<String> path, String name, String definition,
                            OnConflict onConflict, List<String> aliases, String comment,
                            Map<String, String> tags, Map<String, String> columnComments) {
        if (comment == null && (tags == null || tags.isEmpty()) && (columnComments == null || columnComments.isEmpty())) {
            viewCreate(attachId, path, name, definition, onConflict, aliases == null ? List.of() : aliases);
            return;
        }
        if (overrides("viewCreate", byte[].class, List.class, String.class, String.class, OnConflict.class, List.class)
                || overrides("viewCreate", byte[].class, List.class, String.class, String.class, OnConflict.class)) {
            throw new UnsupportedOperationException("catalog " + name() + " does not support view comments or tags:"
                    + " override viewCreate(attachId, path, name, definition, onConflict, aliases, comment, tags,"
                    + " columnComments)");
        }
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

    default void viewCommentSet(byte[] attachId, List<String> path, String name, String comment, boolean ignoreNotFound) {
        throw readOnly("catalog_view_comment_set");
    }

    default void viewColumnCommentSet(byte[] attachId, List<String> path, String name, List<String> columnPath, String comment, boolean ignoreNotFound) {
        throw readOnly("catalog_view_column_comment_set");
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
        return new ReadOnlyCatalogException(
                "catalog " + name() + " is read-only: " + method + " not supported");
    }

    /** Whether this catalog's class overrides the named default method. */
    private boolean overrides(String method, Class<?>... parameters) {
        try {
            return getClass().getMethod(method, parameters).getDeclaringClass() != CatalogInterface.class;
        } catch (NoSuchMethodException e) {
            return false;
        }
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
