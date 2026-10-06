// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import farm.query.vgi.CatalogInterface;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableCreateRequest;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.ViewInfo;
import farm.query.vgirpc.schema.Nullable;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A DDL-capable catalog held in the worker's memory: schemas, tables and views
 * created and dropped through SQL ({@code CREATE SCHEMA / TABLE / VIEW},
 * {@code DROP ...}). The Java counterpart of vgi-python's {@code InMemoryCatalog}.
 *
 * <p>Every change bumps the catalog {@link #version}, which starts at {@code 1},
 * so a client polling the version (or revalidating a {@code catalog_contents}
 * snapshot whose etag derives from it) sees the change. Tables hold metadata
 * only: they can be listed and described but have no rows to scan.</p>
 *
 * <p>State is either private to each ATTACH ({@link Scope#PER_ATTACH} — every
 * ATTACH starts from an empty catalog with one {@code main} schema, and DETACH
 * discards it) or one catalog shared by every attach ({@link Scope#SHARED}).
 * Either way it lives in this worker process, so it needs one long-lived worker
 * (a launcher or HTTP server), not a process per connection.</p>
 *
 * <p>Advertises {@code supports_catalog_contents}; subclasses override
 * {@link #version}, {@link #catalogContents} or {@link #catalogContentsEtag} to
 * change how the catalog revalidates.</p>
 */
public class InMemoryCatalog implements CatalogInterface {

    /** Whether attaches share one catalog or each gets its own. */
    public enum Scope {
        /** Every ATTACH gets a private, initially empty catalog, discarded at DETACH. */
        PER_ATTACH,
        /** Every ATTACH sees the same catalog. */
        SHARED
    }

    /** The default schema every catalog starts with. */
    public static final String DEFAULT_SCHEMA = "main";

    private static final byte[] ITEM_ATTACH_ID = new byte[16];

    /** One schema's objects, in creation order. */
    private static final class SchemaState {
        final List<String> path;
        final @Nullable String comment;
        final Map<String, String> tags;
        final Map<String, TableInfo> tables = new LinkedHashMap<>();
        final Map<String, ViewInfo> views = new LinkedHashMap<>();

        SchemaState(List<String> path, @Nullable String comment, Map<String, String> tags) {
            this.path = List.copyOf(path);
            this.comment = comment;
            this.tags = tags == null ? Map.of() : Map.copyOf(tags);
        }
    }

    /** One catalog: its schemas (keyed by lower-cased path) and version. */
    private static final class State {
        final Map<List<String>, SchemaState> schemas = new LinkedHashMap<>();
        long version = 1;

        State() {
            schemas.put(key(List.of(DEFAULT_SCHEMA)), new SchemaState(List.of(DEFAULT_SCHEMA), null, Map.of()));
        }
    }

    private final String name;
    private final Scope scope;
    private final @Nullable String comment;
    private final Map<String, State> attaches = new ConcurrentHashMap<>();
    private final State shared;

    /**
     * A catalog whose attaches are private ({@link Scope#PER_ATTACH}).
     *
     * @param name the catalog name used in {@code ATTACH '<name>'}
     */
    public InMemoryCatalog(String name) {
        this(name, Scope.PER_ATTACH, null);
    }

    /**
     * A catalog with an explicit scope and comment.
     *
     * @param name    the catalog name used in {@code ATTACH '<name>'}
     * @param scope   private per attach, or shared
     * @param comment the catalog comment, or {@code null}
     */
    public InMemoryCatalog(String name, Scope scope, @Nullable String comment) {
        this.name = java.util.Objects.requireNonNull(name, "name");
        this.scope = java.util.Objects.requireNonNull(scope, "scope");
        this.comment = comment;
        this.shared = scope == Scope.SHARED ? new State() : null;
    }

    @Override
    public String name() { return name; }

    @Override
    public AttachInfo attach(byte[] attachId, CatalogAttachRequest request) {
        if (scope == Scope.PER_ATTACH) attaches.put(id(attachId), new State());
        return new AttachInfo(DEFAULT_SCHEMA, comment, Map.of(), false, true);
    }

    @Override
    public void detach(byte[] attachId) {
        if (scope == Scope.PER_ATTACH) attaches.remove(id(attachId));
    }

    /**
     * The number of private catalogs currently attached (always 0 for {@link Scope#SHARED}).
     *
     * @return the attach count
     */
    public int attachedCount() { return attaches.size(); }

    @Override
    public long version(byte[] attachId) {
        State s = state(attachId);
        synchronized (s) { return s.version; }
    }

    @Override
    public List<SchemaInfo> schemas(byte[] attachId) {
        State s = state(attachId);
        synchronized (s) {
            List<SchemaInfo> out = new ArrayList<>(s.schemas.size());
            for (SchemaState schema : s.schemas.values()) out.add(info(schema));
            out.sort(java.util.Comparator.comparingInt(i -> i.path().size()));
            return out;
        }
    }

    @Override
    public Optional<SchemaInfo> schema(byte[] attachId, List<String> path) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = s.schemas.get(key(path));
            return schema == null ? Optional.empty() : Optional.of(info(schema));
        }
    }

    @Override
    public List<TableInfo> tables(byte[] attachId, List<String> path) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = s.schemas.get(key(path));
            return schema == null ? List.of() : List.copyOf(schema.tables.values());
        }
    }

    @Override
    public List<ViewInfo> views(byte[] attachId, List<String> path) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = s.schemas.get(key(path));
            return schema == null ? List.of() : List.copyOf(schema.views.values());
        }
    }

    @Override
    public Optional<TableInfo> table(byte[] attachId, List<String> path, String name) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = s.schemas.get(key(path));
            return schema == null ? Optional.empty() : Optional.ofNullable(schema.tables.get(name));
        }
    }

    @Override
    public Optional<ViewInfo> view(byte[] attachId, List<String> path, String name) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = s.schemas.get(key(path));
            return schema == null ? Optional.empty() : Optional.ofNullable(schema.views.get(name));
        }
    }

    @Override
    public void schemaCreate(byte[] attachId, List<String> path, OnConflict onConflict,
                             @Nullable String comment, Map<String, String> tags) {
        if (path == null || path.isEmpty()) throw new IllegalArgumentException("Schema path is empty");
        State s = state(attachId);
        synchronized (s) {
            if (s.schemas.containsKey(key(path))) {
                if (onConflict == OnConflict.IGNORE) return;
                if (onConflict != OnConflict.REPLACE) {
                    throw new IllegalArgumentException("Schema " + String.join(".", path) + " already exists");
                }
            }
            if (path.size() > 1 && !s.schemas.containsKey(key(path.subList(0, path.size() - 1)))) {
                throw new IllegalArgumentException(
                        "Schema " + String.join(".", path.subList(0, path.size() - 1)) + " not found");
            }
            s.schemas.put(key(path), new SchemaState(path, comment, tags));
            s.version++;
        }
    }

    @Override
    public void schemaDrop(byte[] attachId, List<String> path, boolean ignoreNotFound, boolean cascade) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = s.schemas.get(key(path));
            if (schema == null) {
                if (ignoreNotFound) return;
                throw new IllegalArgumentException("Schema " + String.join(".", path) + " not found");
            }
            List<List<String>> doomed = new ArrayList<>();
            for (Map.Entry<List<String>, SchemaState> e : s.schemas.entrySet()) {
                List<String> p = e.getKey();
                if (p.size() > path.size() && p.subList(0, path.size()).equals(key(path))) doomed.add(p);
            }
            boolean empty = schema.tables.isEmpty() && schema.views.isEmpty() && doomed.isEmpty();
            if (!cascade && !empty) {
                throw new IllegalArgumentException(
                        "Schema " + String.join(".", path) + " is not empty; use CASCADE to drop it");
            }
            for (List<String> p : doomed) s.schemas.remove(p);
            s.schemas.remove(key(path));
            s.version++;
        }
    }

    @Override
    public void tableCreate(byte[] attachId, TableCreateRequest request) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = requireSchema(s, request.schema_path());
            if (schema.tables.containsKey(request.name())
                    && !replace("Table", request.name(), OnConflict.fromWire(request.on_conflict()))) {
                return;
            }
            schema.tables.put(request.name(), new TableInfo(
                    null, Map.of(), request.name(), schema.path,
                    request.columns() == null ? new byte[0] : request.columns(),
                    request.not_null_constraints(), request.unique_constraints(),
                    request.check_constraints(), request.primary_key_constraints(),
                    request.foreign_key_constraints(), Map.of(), false,
                    null, null, null, null, null, null, null, null, List.of()));
            s.version++;
        }
    }

    @Override
    public void tableDrop(byte[] attachId, List<String> path, String name, boolean ignoreNotFound,
                          boolean cascade) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = s.schemas.get(key(path));
            if (schema == null || schema.tables.remove(name) == null) {
                if (ignoreNotFound) return;
                throw new IllegalArgumentException("Table " + name + " not found in schema " + String.join(".", path));
            }
            s.version++;
        }
    }

    @Override
    public void viewCreate(byte[] attachId, List<String> path, String name, String definition,
                           OnConflict onConflict) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = requireSchema(s, path);
            if (schema.views.containsKey(name) && !replace("View", name, onConflict)) return;
            schema.views.put(name, new ViewInfo(null, Map.of(), name, schema.path, definition, Map.of()));
            s.version++;
        }
    }

    @Override
    public void viewDrop(byte[] attachId, List<String> path, String name, boolean ignoreNotFound,
                         boolean cascade) {
        State s = state(attachId);
        synchronized (s) {
            SchemaState schema = s.schemas.get(key(path));
            if (schema == null || schema.views.remove(name) == null) {
                if (ignoreNotFound) return;
                throw new IllegalArgumentException("View " + name + " not found in schema " + String.join(".", path));
            }
            s.version++;
        }
    }

    /**
     * The catalog state of an attach.
     *
     * @throws IllegalStateException when the attach is unknown (detached, or a worker restart)
     */
    private State state(byte[] attachId) {
        if (shared != null) return shared;
        State s = attaches.get(id(attachId));
        if (s == null) throw new IllegalStateException(name + ": not attached (detached, or the worker restarted)");
        return s;
    }

    private static SchemaState requireSchema(State s, List<String> path) {
        SchemaState schema = s.schemas.get(key(path));
        if (schema == null) throw new IllegalArgumentException("Schema " + String.join(".", path) + " not found");
        return schema;
    }

    /** True to replace an existing object, false to keep it; throws on a plain conflict. */
    private static boolean replace(String kind, String name, OnConflict onConflict) {
        return switch (onConflict) {
            case IGNORE -> false;
            case REPLACE -> true;
            case ERROR -> throw new IllegalArgumentException(kind + " " + name + " already exists");
        };
    }

    /**
     * The listing for a schema. Functions and indexes cannot exist here, and
     * this catalog has no macro DDL, so those counts are an explicit {@code 0}:
     * the client skips their listing RPCs (and {@code catalog_contents} skips them too).
     */
    private static SchemaInfo info(SchemaState schema) {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("table", (long) schema.tables.size());
        counts.put("view", (long) schema.views.size());
        counts.put("macro", 0L);
        counts.put("index", 0L);
        counts.put("scalar_function", 0L);
        counts.put("aggregate_function", 0L);
        counts.put("table_function", 0L);
        return new SchemaInfo(schema.comment, schema.tags, ITEM_ATTACH_ID.clone(), schema.path, counts);
    }

    private static List<String> key(List<String> path) {
        List<String> k = new ArrayList<>(path.size());
        for (String p : path) k.add(p.toLowerCase(Locale.ROOT));
        return List.copyOf(k);
    }

    private static String id(byte[] attachId) {
        return HexFormat.of().formatHex(attachId);
    }
}
