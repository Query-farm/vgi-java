// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import farm.query.vgi.AttachOptionSpec;
import farm.query.vgi.CatalogDataVersionRelease;
import farm.query.vgi.Worker;
import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.View;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.internal.MacroArgumentsSchema;
import farm.query.vgi.internal.MacroDefaultsEncoder;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.table.CopyFromFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.types.TypeRules;
import farm.query.vgirpc.http.DescribeProvider;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Produces the standardized HTTP landing contract ({@code describe.json} and the
 * lazy per-object column endpoints) from a {@link Worker}'s in-memory catalog
 * model. This is the Java counterpart to vgi-python's
 * {@code vgi/http/describe_json.py}; the shared static {@code landing.html} —
 * byte-identical across every VGI language worker — fetches these same-origin
 * and renders them.
 *
 * <p>See {@code ~/Development/vgi/docs/http-landing-contract.md} for the
 * normative spec. The document is versioned by {@code landing_schema_version}
 * independently of the VGI wire protocol; table/view columns are lazy (only a
 * count is carried in {@code describe.json}, the page fetches per-object detail
 * from {@link #columnsJson}).</p>
 */
public final class WorkerDescribeProvider implements DescribeProvider {

    /** This contract's version (see the landing contract doc). */
    private static final int LANDING_SCHEMA_VERSION = 1;
    private static final String CUPOLA_BASE = "https://cupola.query-farm.services";

    /** Contract catalog-tag keys and the raw {@code duckdb_databases().tags}
     *  keys they map to (reserved {@code vgi.*} namespace). */
    private static final Map<String, String> STRING_TAGS = orderedTags();

    private static Map<String, String> orderedTags() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("title", "vgi.title");
        m.put("doc_md", "vgi.doc_md");
        m.put("source_url", "vgi.source_url");
        m.put("license", "vgi.license");
        m.put("author", "vgi.author");
        m.put("copyright", "vgi.copyright");
        m.put("support_contact", "vgi.support_contact");
        m.put("support_policy_url", "vgi.support_policy_url");
        return m;
    }

    private static final String KEYWORDS_TAG = "vgi.keywords";

    /** Serializes nulls so the required nullable fields
     *  ({@code implementation_version}, {@code data_version_spec}) are always
     *  present; every other value the builder emits is non-null. */
    private static final Gson GSON = new GsonBuilder().serializeNulls().create();

    private final Worker worker;
    private final String workerName;
    private final String workerDoc;
    private final String workerVersion;

    /**
     * Build a provider for {@code worker}, deriving the worker name from the
     * catalog name, the doc from the catalog comment, and the version from the
     * VGI core package manifest.
     *
     * @param worker the worker to introspect
     */
    public WorkerDescribeProvider(Worker worker) {
        this(worker, worker.catalogName(), firstLine(worker.catalogComment()), packageVersion());
    }

    /**
     * Build a provider with explicit worker identity fields.
     *
     * @param worker        the worker to introspect
     * @param workerName    the worker name surfaced as {@code worker.name}
     * @param workerDoc     the one-line worker description
     * @param workerVersion the worker software version
     */
    public WorkerDescribeProvider(Worker worker, String workerName, String workerDoc, String workerVersion) {
        this.worker = worker;
        this.workerName = workerName == null ? "" : workerName;
        this.workerDoc = workerDoc == null ? "" : workerDoc;
        this.workerVersion = workerVersion == null ? "unknown" : workerVersion;
    }

    @Override
    public String describeJson(String serverId, boolean oauthActive) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("landing_schema_version", LANDING_SCHEMA_VERSION);
        Map<String, Object> w = new LinkedHashMap<>();
        w.put("name", workerName);
        w.put("doc", workerDoc);
        w.put("version", workerVersion);
        w.put("lang", "java");
        doc.put("worker", w);
        doc.put("server_id", serverId == null ? "" : serverId);
        doc.put("oauth", oauthActive);
        doc.put("cupola_base", CUPOLA_BASE);
        // One entry per catalog the worker advertises via catalog_catalogs():
        // the main catalog, then each auxiliary (MetaWorker-style) catalog. Each
        // carries only its own catalog-scoped objects.
        List<Map<String, Object>> catalogs = new ArrayList<>();
        catalogs.add(buildMainCatalog());
        for (Worker.ExtraCatalog extra : worker.extraCatalogs().values()) {
            catalogs.add(buildExtraCatalog(extra));
        }
        doc.put("catalogs", catalogs);
        return GSON.toJson(doc);
    }

    @Override
    public String columnsJson(String catalog, String schema, String table) {
        List<CatalogTable> tableSource;
        List<View> viewSource;
        if (catalog.equals(worker.catalogName())) {
            tableSource = worker.catalogTables();
            viewSource = worker.views();
        } else if (worker.extraCatalogs().containsKey(catalog)) {
            // Auxiliary catalogs advertise only a "main" schema of owned tables
            // (no views).
            tableSource = worker.extraCatalogTables().getOrDefault(catalog, List.of());
            viewSource = List.of();
        } else {
            return null;
        }
        for (CatalogTable t : tableSource) {
            if (t.schema().equals(schema) && t.name().equals(table)) {
                List<Map<String, Object>> cols = new ArrayList<>();
                Schema s = deserialize(t.columns());
                if (s != null) {
                    for (Field f : s.getFields()) {
                        cols.add(columnEntry(f));
                    }
                }
                return GSON.toJson(Map.of("columns", cols));
            }
        }
        for (View v : viewSource) {
            if (v.schema().equals(schema) && v.name().equals(table)) {
                List<Map<String, Object>> cols = new ArrayList<>();
                for (Map.Entry<String, String> e : v.columnComments().entrySet()) {
                    Map<String, Object> col = new LinkedHashMap<>();
                    col.put("name", e.getKey());
                    // View column types are only known after binding the SQL,
                    // which the worker does not do here (mirrors the Python ref).
                    col.put("type", "");
                    if (e.getValue() != null && !e.getValue().isEmpty()) {
                        col.put("comment", e.getValue());
                    }
                    cols.add(col);
                }
                return GSON.toJson(Map.of("columns", cols));
            }
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Catalog assembly
    // -----------------------------------------------------------------------

    private Map<String, Object> buildMainCatalog() {
        Map<String, Object> cat = new LinkedHashMap<>();
        cat.put("name", worker.catalogName());
        cat.put("implementation_version", worker.implementationVersion());
        cat.put("data_version_spec", worker.dataVersionSpec());

        List<Map<String, Object>> dataVersions = new ArrayList<>();
        for (CatalogDataVersionRelease r : worker.releases()) {
            Map<String, Object> dv = new LinkedHashMap<>();
            dv.put("spec", r.version());
            if (r.summary() != null && !r.summary().isEmpty()) dv.put("label", r.summary());
            dataVersions.add(dv);
        }
        cat.put("data_versions", dataVersions);

        List<Map<String, Object>> attachOptions = new ArrayList<>();
        for (AttachOptionSpec s : worker.attachOptionSpecs()) {
            Map<String, Object> ao = new LinkedHashMap<>();
            ao.put("name", s.name());
            ao.put("type", TypeRules.sqlTypeName(s.type()));
            ao.put("default", attachDefault(s));
            ao.put("description", s.description() == null ? "" : s.description());
            attachOptions.add(ao);
        }
        cat.put("attach_options", attachOptions);
        cat.put("tags", buildTags());

        // Schema order: default schema first, then any schema seen on a
        // catalog table or view, in encounter order.
        Set<String> schemaNames = new LinkedHashSet<>();
        schemaNames.add(worker.defaultSchema());
        for (CatalogTable t : worker.catalogTables()) schemaNames.add(t.schema());
        for (View v : worker.views()) schemaNames.add(v.schema());

        Set<String> extraPrefixes = new LinkedHashSet<>();
        for (Worker.ExtraCatalog ec : worker.extraCatalogs().values()) {
            if (ec.functionNamePrefix() != null && !ec.functionNamePrefix().isEmpty()) {
                extraPrefixes.add(ec.functionNamePrefix());
            }
        }

        Map<String, String> schemaComments = worker.schemaComments();

        int nSchemas = 0, nTables = 0, nViews = 0, nFunctions = 0;
        List<Map<String, Object>> schemas = new ArrayList<>();
        for (String schemaName : schemaNames) {
            Map<String, Object> sch = new LinkedHashMap<>();
            sch.put("name", schemaName);
            String schemaDoc = schemaComments.get(schemaName);
            if (schemaDoc != null && !schemaDoc.isEmpty()) sch.put("doc", schemaDoc);

            List<Map<String, Object>> tables = new ArrayList<>();
            for (CatalogTable t : worker.catalogTables()) {
                if (!t.schema().equals(schemaName)) continue;
                Map<String, Object> tm = new LinkedHashMap<>();
                tm.put("name", t.name());
                tm.put("cols", columnCount(t.columns()));
                tm.put("comment", t.comment() == null ? "" : t.comment());
                tables.add(tm);
            }
            sch.put("tables", tables);

            List<Map<String, Object>> views = new ArrayList<>();
            for (View v : worker.views()) {
                if (!v.schema().equals(schemaName)) continue;
                Map<String, Object> vm = new LinkedHashMap<>();
                vm.put("name", v.name());
                vm.put("cols", v.columnComments().size());
                vm.put("comment", v.comment() == null ? "" : v.comment());
                vm.put("def", v.definition() == null ? "" : v.definition());
                views.add(vm);
            }
            sch.put("views", views);

            List<Map<String, Object>> functions =
                    schemaName.equals(worker.defaultSchema()) ? buildFunctions(extraPrefixes) : List.of();
            sch.put("functions", functions);

            schemas.add(sch);
            nSchemas++;
            nTables += tables.size();
            nViews += views.size();
            nFunctions += functions.size();
        }

        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("schemas", nSchemas);
        counts.put("tables", nTables);
        counts.put("views", nViews);
        counts.put("functions", nFunctions);
        cat.put("counts", counts);
        cat.put("schemas", schemas);
        return cat;
    }

    /**
     * Build a catalog entry for an auxiliary (MetaWorker-style) catalog, scoped
     * to the objects it owns. Mirrors the RPC's per-attach scoping
     * ({@code VgiServiceImpl#catalog_schemas} / {@code
     * catalog_schema_contents_*} for an extra attach): a single {@code main}
     * schema carrying the catalog's own tables and the table-type functions
     * whose names match its prefix (no scalars, aggregates, or views).
     */
    private Map<String, Object> buildExtraCatalog(Worker.ExtraCatalog extra) {
        Map<String, Object> cat = new LinkedHashMap<>();
        cat.put("name", extra.name());
        cat.put("implementation_version", extra.implementationVersion());
        cat.put("data_version_spec", extra.dataVersion());
        cat.put("data_versions", List.of());
        cat.put("attach_options", List.of());
        cat.put("tags", new LinkedHashMap<>());

        List<Map<String, Object>> tables = new ArrayList<>();
        for (CatalogTable t : worker.extraCatalogTables().getOrDefault(extra.name(), List.of())) {
            // Auxiliary catalogs advertise only their "main" schema.
            if (!"main".equals(t.schema())) continue;
            Map<String, Object> tm = new LinkedHashMap<>();
            tm.put("name", t.name());
            tm.put("cols", columnCount(t.columns()));
            tm.put("comment", t.comment() == null ? "" : t.comment());
            tables.add(tm);
        }
        List<Map<String, Object>> functions = buildExtraFunctions(extra.functionNamePrefix());

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("name", "main");
        schema.put("tables", tables);
        schema.put("views", List.of());
        schema.put("functions", functions);

        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("schemas", 1);
        counts.put("tables", tables.size());
        counts.put("views", 0);
        counts.put("functions", functions.size());
        cat.put("counts", counts);
        cat.put("schemas", List.of(schema));
        return cat;
    }

    /** Table-type functions (table / table-in-out / buffering) whose names match
     *  an auxiliary catalog's {@code prefix}. Scalars and aggregates are never
     *  advertised by an auxiliary catalog (parity with the RPC). */
    private List<Map<String, Object>> buildExtraFunctions(String prefix) {
        List<Map<String, Object>> functions = new ArrayList<>();
        if (prefix == null || prefix.isEmpty()) return functions;
        for (TableFunction fn : worker.tables()) {
            if (!fn.name().startsWith(prefix)) continue;
            functions.add(function(fn.name(), "table", fn.metadata().description(),
                    fn.argumentSpecs(), tableReturns(fn)));
        }
        for (TableInOutFunction fn : worker.tableInOuts()) {
            if (!fn.name().startsWith(prefix)) continue;
            functions.add(function(fn.name(), "table_in_out", fn.metadata().description(),
                    fn.argumentSpecs(), tableInOutReturns(fn)));
        }
        for (TableBufferingFunction fn : worker.bufferingFunctions()) {
            if (!fn.name().startsWith(prefix)) continue;
            functions.add(function(fn.name(), "table_in_out", fn.metadata().description(),
                    fn.argumentSpecs(), bufferingReturns(fn)));
        }
        return functions;
    }

    private Map<String, Object> buildTags() {
        Map<String, String> tags = worker.catalogTags();
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : STRING_TAGS.entrySet()) {
            String key = e.getKey();
            String val = tagValue(tags, key, e.getValue());
            if (val != null && !val.isEmpty()) out.put(key, val);
        }
        // source_url falls back to the worker-level sourceUrl() when no tag set it.
        if (!out.containsKey("source_url") && worker.sourceUrl() != null && !worker.sourceUrl().isEmpty()) {
            out.put("source_url", worker.sourceUrl());
        }
        String kw = tags.get(KEYWORDS_TAG);
        if (kw == null) kw = tags.get("keywords");
        if (kw != null && !kw.isEmpty()) {
            try {
                String[] parsed = GSON.fromJson(kw, String[].class);
                if (parsed != null && parsed.length > 0) out.put("keywords", List.of(parsed));
            } catch (JsonSyntaxException ignore) {
                // Not a JSON array — omit rather than mis-render.
            }
        }
        return out;
    }

    private static String tagValue(Map<String, String> tags, String key, String vgiKey) {
        String v = tags.get(vgiKey);
        if (v == null) v = tags.get(key);
        return v;
    }

    // -----------------------------------------------------------------------
    // Functions
    // -----------------------------------------------------------------------

    private List<Map<String, Object>> buildFunctions(Set<String> extraPrefixes) {
        List<Map<String, Object>> functions = new ArrayList<>();
        for (ScalarFunction fn : worker.scalars()) {
            if (hidden(fn.name(), extraPrefixes)) continue;
            functions.add(function(fn.name(), "scalar", fn.metadata().description(),
                    fn.argumentSpecs(), scalarReturns(fn)));
        }
        for (TableFunction fn : worker.tables()) {
            if (hidden(fn.name(), extraPrefixes)) continue;
            functions.add(function(fn.name(), "table", fn.metadata().description(),
                    fn.argumentSpecs(), tableReturns(fn)));
        }
        for (TableInOutFunction fn : worker.tableInOuts()) {
            if (hidden(fn.name(), extraPrefixes)) continue;
            functions.add(function(fn.name(), "table_in_out", fn.metadata().description(),
                    fn.argumentSpecs(), tableInOutReturns(fn)));
        }
        for (AggregateFunction<?> fn : worker.aggregates()) {
            if (hidden(fn.name(), extraPrefixes)) continue;
            functions.add(function(fn.name(), "aggregate", fn.metadata().description(),
                    fn.argumentSpecs(), aggregateReturns(fn)));
        }
        for (TableBufferingFunction fn : worker.bufferingFunctions()) {
            if (hidden(fn.name(), extraPrefixes)) continue;
            functions.add(function(fn.name(), "table_in_out", fn.metadata().description(),
                    fn.argumentSpecs(), bufferingReturns(fn)));
        }
        // Catalog macros (scalar + table) fold into the same functions array: a
        // scalar macro is invoked exactly like a scalar function in SQL, a table
        // macro like a table function, so the landing page lists a catalog's full
        // callable surface (VGI workers commonly expose "functions" as declarative
        // macros). Mirrors vgi-python's describe_json._build_schemas.
        for (Macro m : worker.macros()) {
            if (hidden(m.name(), extraPrefixes)) continue;
            functions.add(macro(m));
        }
        // Deterministic ordering across functions + macros (both fold into the
        // same scalar/table/aggregate/table_in_out buckets), matching the Python
        // reference producer's sort by (type, name).
        functions.sort(Comparator.comparing((Map<String, Object> d) -> (String) d.get("type"))
                .thenComparing(d -> (String) d.get("name")));
        return functions;
    }

    /** Render a catalog macro as a functions-array entry: a scalar macro maps to
     *  {@code "scalar"}, a table macro to {@code "table"}. Macros carry no
     *  {@code returns} (the body's type is only known after DuckDB expands it). */
    private static Map<String, Object> macro(Macro m) {
        Map<String, Object> fn = new LinkedHashMap<>();
        fn.put("name", m.name());
        fn.put("type", m.macroType() == MacroType.SCALAR ? "scalar" : "table");
        fn.put("doc", m.comment() == null ? "" : m.comment());
        fn.put("args", macroArgs(m));
        return fn;
    }

    /** One arg per macro parameter, in declaration order. A parameter's type is
     *  the Arrow type of its default value when one is known (rendered via the
     *  same {@link TypeRules#sqlTypeName} mapping function args use), else
     *  {@code "ANY"}. Defaulted parameters are optional and callable by name, so
     *  they carry {@code named: true} and the default as its typed JSON scalar. */
    private static List<Map<String, Object>> macroArgs(Macro m) {
        Map<String, String> defaults = m.parameterDefaults() == null ? Map.of() : m.parameterDefaults();
        Schema schema = MacroArgumentsSchema.build(m.parameters(), defaults, m.parameterDocs());
        List<Map<String, Object>> args = new ArrayList<>();
        for (Field f : schema.getFields()) {
            String name = f.getName();
            Map<String, Object> arg = new LinkedHashMap<>();
            arg.put("name", name);
            ArrowType type = f.getType();
            arg.put("type", type instanceof ArrowType.Null ? "ANY" : TypeRules.sqlTypeName(type));
            Map<String, String> md = f.getMetadata();
            String doc = md == null ? null : md.get("vgi_doc");
            if (doc != null && !doc.isEmpty()) arg.put("desc", doc);
            if (defaults.containsKey(name)) {
                arg.put("named", true);
                arg.put("default", GSON.toJson(MacroDefaultsEncoder.valueForLiteral(defaults.get(name))));
            }
            args.add(arg);
        }
        return args;
    }

    private static boolean hidden(String name, Set<String> extraPrefixes) {
        // proj_repro_* belong to the projection_repro catalog, not the main one.
        if (name.startsWith("proj_repro_")) return true;
        for (String p : extraPrefixes) {
            if (name.startsWith(p)) return true;
        }
        return false;
    }

    private static Map<String, Object> function(String name, String type, String doc,
                                                 List<ArgSpec> specs, String returns) {
        Map<String, Object> fn = new LinkedHashMap<>();
        fn.put("name", name);
        fn.put("type", type);
        fn.put("doc", doc == null ? "" : doc);
        fn.put("args", buildArgs(specs));
        if (returns != null && !returns.isEmpty()) fn.put("returns", returns);
        return fn;
    }

    private static List<Map<String, Object>> buildArgs(List<ArgSpec> specs) {
        List<ArgSpec> sorted = new ArrayList<>(specs);
        // Positional first (by position), named after — matches ArgumentSpecSerializer.
        sorted.sort((a, b) -> {
            int ka = a.position() >= 0 ? a.position() : Integer.MAX_VALUE;
            int kb = b.position() >= 0 ? b.position() : Integer.MAX_VALUE;
            return Integer.compare(ka, kb);
        });
        List<Map<String, Object>> args = new ArrayList<>();
        for (ArgSpec spec : sorted) {
            // Skip the piped input relation of a table-in-out function; it's not
            // a user-supplied argument.
            if (spec.tableInput()) continue;
            Map<String, Object> arg = new LinkedHashMap<>();
            arg.put("name", spec.name());
            arg.put("type", spec.anyType() ? "ANY" : TypeRules.sqlTypeName(spec.arrowType()));
            if (spec.position() < 0) arg.put("named", true);
            if (spec.doc() != null && !spec.doc().isEmpty()) arg.put("desc", spec.doc());
            if (spec.hasDefault() && spec.defaultValue() != null && !spec.defaultValue().isEmpty()) {
                arg.put("default", spec.defaultValue());
            }
            args.add(arg);
        }
        return args;
    }

    // --- returns rendering (best-effort; null on any failure) ---------------

    private static String scalarReturns(ScalarFunction fn) {
        try {
            BindResponse r = fn.onBind(new ScalarBindParams(fn.name(), Arguments.empty(), null, Map.of()));
            Schema s = deserialize(bindOutput(r));
            return (s == null || s.getFields().isEmpty()) ? null
                    : TypeRules.sqlTypeName(s.getFields().get(0).getType());
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String tableReturns(TableFunction fn) {
        if (fn instanceof CopyFromFunction) return null; // no static output schema
        try {
            BindResponse r = fn.onBind(new TableBindParams(fn.name(), Arguments.empty(), null, Map.of()));
            return tableType(deserialize(bindOutput(r)));
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String tableInOutReturns(TableInOutFunction fn) {
        try {
            BindResponse r = fn.onBind(new TableInOutBindParams(fn.name(), Arguments.empty(), null, Map.of()));
            return tableType(deserialize(bindOutput(r)));
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String bufferingReturns(TableBufferingFunction fn) {
        try {
            BindResponse r = fn.onBind(new TableInOutBindParams(fn.name(), Arguments.empty(), null, Map.of()));
            return tableType(deserialize(bindOutput(r)));
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String aggregateReturns(AggregateFunction<?> fn) {
        try {
            Schema s = fn.outputSchema();
            return (s == null || s.getFields().isEmpty()) ? null
                    : TypeRules.sqlTypeName(s.getFields().get(0).getType());
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String tableType(Schema s) {
        if (s == null || s.getFields().isEmpty()) return null;
        StringBuilder sb = new StringBuilder("TABLE(");
        for (int i = 0; i < s.getFields().size(); i++) {
            Field f = s.getFields().get(i);
            if (i > 0) sb.append(", ");
            sb.append(f.getName()).append(' ').append(TypeRules.sqlTypeName(f.getType()));
        }
        return sb.append(')').toString();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static byte[] bindOutput(BindResponse r) {
        return r != null && r.output_schema() != null ? r.output_schema() : new byte[0];
    }

    private static Schema deserialize(byte[] ipc) {
        if (ipc == null || ipc.length == 0) return null;
        try {
            return SchemaUtil.deserializeSchema(ipc);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static int columnCount(byte[] ipc) {
        Schema s = deserialize(ipc);
        return s == null ? 0 : s.getFields().size();
    }

    private static Map<String, Object> columnEntry(Field f) {
        Map<String, Object> col = new LinkedHashMap<>();
        col.put("name", f.getName());
        col.put("type", TypeRules.sqlTypeName(f.getType()));
        Map<String, String> md = f.getMetadata();
        String comment = md == null ? null : md.getOrDefault("comment", md.get("vgi_doc"));
        if (comment != null && !comment.isEmpty()) col.put("comment", comment);
        return col;
    }

    private static String attachDefault(AttachOptionSpec s) {
        FieldVector v = s.defaultVector();
        if (v == null || v.getValueCount() == 0 || v.isNull(0)) return "";
        Object o = v.getObject(0);
        return o == null ? "" : String.valueOf(o);
    }

    private static String firstLine(String s) {
        if (s == null || s.isEmpty()) return "";
        int nl = s.indexOf('\n');
        return (nl < 0 ? s : s.substring(0, nl)).strip();
    }

    private static String packageVersion() {
        String v = Worker.class.getPackage() == null ? null
                : Worker.class.getPackage().getImplementationVersion();
        return v == null ? "unknown" : v;
    }
}
