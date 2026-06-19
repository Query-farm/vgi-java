// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.View;
import farm.query.vgi.internal.VgiServiceImpl;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.transport.StdioTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;

import java.io.IOException;
import java.nio.file.Path;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder + run-loop façade for a VGI worker.
 *
 * <p>Mirrors {@code vgi.Worker} in vgi-go: register functions, configure catalog
 * metadata, then call {@link #runStdio()} or {@link #runHttp(String, int)}.
 */
public final class Worker {

    /** VGI protocol surface version. Mirrors vgi-python {@code protocol_version.txt}.
     *  Emitted as the {@code vgi_rpc.protocol_version} per-request metadata key. */
    public static final String VGI_PROTOCOL_VERSION = "1.0.0";

    private String catalogName = "vgi";
    private String catalogComment = "";
    private final Map<String, String> catalogTags = new LinkedHashMap<>();
    private String defaultSchema = "main";
    private final Map<String, String> schemaComments = new LinkedHashMap<>();
    private String implementationVersion;
    private String dataVersionSpec;
    private final List<CatalogDataVersionRelease> releases = new ArrayList<>();
    private String sourceUrl;
    /** Optional 32-byte ChaCha20-Poly1305 key for sealing attach / transaction
     *  opaque data. {@code null} ⇒ per-process random (single-replica only).
     *  See {@link #opaqueDataKey(byte[])}. */
    private byte[] opaqueDataKey;
    private final List<ScalarFunction> scalars = new ArrayList<>();
    private final List<TableFunction> tables = new ArrayList<>();
    private final List<TableInOutFunction> tableInOuts = new ArrayList<>();
    private final List<farm.query.vgi.buffering.TableBufferingFunction> bufferingFns = new ArrayList<>();
    private final List<AggregateFunction<?>> aggregates = new ArrayList<>();
    private final List<SettingSpec> settings = new ArrayList<>();
    private final List<SecretTypeSpec> secretTypes = new ArrayList<>();
    private final List<AttachOptionSpec> attachOptions = new ArrayList<>();
    private final List<View> views = new ArrayList<>();

    private final List<Macro> macros = new ArrayList<>();

    /**
     * Register a SQL macro.
     *
     * @param m the macro to register
     * @return this builder
     */
    public Worker registerMacro(Macro m) {
        macros.add(m);
        return this;
    }

    /**
     * Register several SQL macros.
     *
     * @param ms the macros to register
     * @return this builder
     */
    public Worker registerMacros(Iterable<? extends Macro> ms) {
        for (Macro m : ms) macros.add(m);
        return this;
    }

    /**
     * Macros enumerated through {@code catalog_schema_contents_macros}.
     *
     * @return the registered macros, in registration order
     */
    public List<Macro> macros() { return macros; }

    private final List<CatalogTable> catalogTables = new ArrayList<>();
    private final Map<String, List<farm.query.vgi.catalog.ScanBranch>> multiBranchTables =
            new LinkedHashMap<>();

    /**
     * Register a catalog table.
     *
     * @param t the table to register
     * @return this builder
     */
    public Worker registerCatalogTable(CatalogTable t) {
        catalogTables.add(t);
        return this;
    }

    /**
     * Catalog tables enumerated through {@code catalog_schema_contents_tables} /
     * {@code catalog_table_get}.
     *
     * @return the registered catalog tables, in registration order
     */
    public List<CatalogTable> catalogTables() { return catalogTables; }

    /**
     * Register a multi-branch table: a catalog table whose scan is the
     * {@code UNION_ALL} of {@code branches}. The table is enumerated normally;
     * its scan resolves through {@code catalog_table_scan_branches_get}. Pass
     * an empty branch list to exercise the C++ loud-fail path. The stub's
     * inline scan-function (if any) is dropped so the branches RPC drives.
     *
     * @param stub     the catalog table to enumerate (its inline scan is dropped)
     * @param branches the branches whose {@code UNION_ALL} forms the scan; empty exercises the loud-fail path
     * @return this builder
     */
    public Worker registerMultiBranchTable(CatalogTable stub,
            List<farm.query.vgi.catalog.ScanBranch> branches) {
        catalogTables.add(stub.withRpcScanFunction());
        multiBranchTables.put(stub.schema() + "." + stub.name(),
                branches == null ? List.of() : List.copyOf(branches));
        return this;
    }

    /**
     * Branches for a multi-branch table.
     *
     * @param schema the schema name
     * @param name   the table name
     * @return the registered branches, or {@code null} if the table is not multi-branch
     */
    public List<farm.query.vgi.catalog.ScanBranch> multiBranchTable(String schema, String name) {
        return multiBranchTables.get(schema + "." + name);
    }

    /**
     * Every table registered via
     * {@link #registerMultiBranchTable(CatalogTable, List)}, used by the
     * service to answer {@code catalog_table_scan_branches_get}.
     *
     * @return all multi-branch tables keyed by {@code schema.name}
     */
    public Map<String, List<farm.query.vgi.catalog.ScanBranch>> multiBranchTables() {
        return multiBranchTables;
    }

    private Worker() {}

    /**
     * Start building a worker. Defaults: catalog name {@code "vgi"}, default
     * schema {@code "main"}, empty comment/tags, no versioning metadata.
     *
     * @return a fresh worker builder with default catalog metadata
     */
    public static Worker builder() { return new Worker(); }

    /**
     * Name this worker's catalog. Surfaced as the catalog row in
     * {@code catalog_catalogs()} and as the default database alias on ATTACH.
     *
     * @param name the catalog name (default {@code "vgi"})
     * @return this builder
     */
    public Worker catalogName(String name) { this.catalogName = name; return this; }

    /**
     * Set the catalog-level comment, surfaced through {@code catalog_catalogs()}
     * and DuckDB's {@code duckdb_databases()} comment column.
     *
     * @param comment the catalog comment (default empty)
     * @return this builder
     */
    public Worker catalogComment(String comment) { this.catalogComment = comment; return this; }

    /**
     * Attach key/value tags to the catalog, surfaced through
     * {@code catalog_catalogs()}. Merged into any previously set tags
     * (later calls overwrite duplicate keys).
     *
     * @param tags catalog tag key/value pairs to merge in
     * @return this builder
     */
    public Worker catalogTags(Map<String, String> tags) { this.catalogTags.putAll(tags); return this; }

    /**
     * Advertise the worker's implementation (code) version, reported through
     * {@code catalog_version} alongside the resolved data version so clients
     * can distinguish "what code is running" from "what data it serves".
     *
     * @param v semver implementation version string (e.g. {@code "11.0.0"});
     *          {@code null} (the default) omits it
     * @return this builder
     */
    public Worker implementationVersion(String v) { this.implementationVersion = v; return this; }

    /**
     * Declare the range of data versions this worker can serve. ATTACH-time
     * version requests are validated against this spec; requests outside the
     * range are rejected.
     *
     * @param v semver range spec (e.g. {@code ">=1.0.0,<4.0.0"});
     *          {@code null} (the default) disables version negotiation
     * @return this builder
     */
    public Worker dataVersionSpec(String v) { this.dataVersionSpec = v; return this; }

    /**
     * Implementation version advertised through {@code catalog_version}.
     *
     * @return the configured implementation version, or {@code null} if unset
     */
    public String implementationVersion() { return implementationVersion; }

    /**
     * Data-version range this worker accepts at ATTACH time.
     *
     * @return the configured data-version spec, or {@code null} if unset
     */
    public String dataVersionSpec() { return dataVersionSpec; }

    /**
     * Provide a stable 32-byte key for sealing attach / transaction
     * {@code opaque_data}. Required when running the same worker across
     * multiple HTTP replicas: without it each replica generates its own
     * random key, and a load balancer rotating across them will surface
     * {@code AEADBadTagException} when one replica receives a blob another
     * replica sealed.
     *
     * <p>{@code null} (the default) restores the per-process random-key
     * behaviour, which is correct for single-replica HTTP and irrelevant
     * for stdio / AF_UNIX (where the sealer is disabled entirely).
     *
     * @param key 32-byte ChaCha20-Poly1305 key, or {@code null} for per-process random
     * @return this builder
     * @throws IllegalArgumentException if {@code key} is non-null but not 32 bytes
     */
    public Worker opaqueDataKey(byte[] key) {
        if (key != null && key.length != 32) {
            throw new IllegalArgumentException(
                    "opaqueDataKey must be 32 bytes (got " + key.length + ")");
        }
        this.opaqueDataKey = key != null ? key.clone() : null;
        return this;
    }

    /** Published data-version releases, surfaced through {@code catalog_catalogs()}.
     *  Pass newest-first.
     *
     * @param rs the releases, newest-first
     * @return this builder
     */
    public Worker releases(CatalogDataVersionRelease... rs) {
        for (CatalogDataVersionRelease r : rs) releases.add(r);
        return this;
    }
    /**
     * Data-version releases surfaced through {@code catalog_catalogs()}.
     *
     * @return a copy of the configured releases, in the order passed to
     *         {@link #releases(CatalogDataVersionRelease...)}
     */
    public List<CatalogDataVersionRelease> releases() { return List.copyOf(releases); }

    /**
     * Set the catalog's source URL (e.g. a homepage or repository link),
     * surfaced through {@code catalog_catalogs()}.
     *
     * @param url the source URL; {@code null} (the default) omits it
     * @return this builder
     */
    public Worker sourceUrl(String url) { this.sourceUrl = url; return this; }

    /**
     * Source URL surfaced through {@code catalog_catalogs()}.
     *
     * @return the configured source URL, or {@code null} if unset
     */
    public String sourceUrl() { return sourceUrl; }

    /**
     * Name the schema DuckDB selects by default after ATTACH. Functions,
     * tables and views without an explicit schema register here.
     *
     * @param schema the default schema name (default {@code "main"})
     * @return this builder
     */
    public Worker defaultSchema(String schema) { this.defaultSchema = schema; return this; }

    /** Per-schema comment surfaced via {@code catalog_schemas} /
     *  {@code catalog_schema_get}. Default comment for the default schema is
     *  "Default schema"; any auxiliary schema without an entry gets an empty
     *  comment.
     *
     * @param schema  the schema name
     * @param comment the comment ({@code null} treated as empty)
     * @return this builder
     */
    public Worker schemaComment(String schema, String comment) {
        schemaComments.put(schema, comment == null ? "" : comment);
        return this;
    }

    /**
     * Comments registered via {@link #schemaComment(String, String)}.
     *
     * @return the per-schema comments keyed by schema name
     */
    public Map<String, String> schemaComments() { return schemaComments; }

    /**
     * An auxiliary catalog served by the same worker process next to the main
     * catalog, MetaWorker-style: it appears as its own row in
     * {@code catalog_catalogs()}, attaches by name with its own versions and a
     * random per-ATTACH opaque id, and owns every registered function whose
     * name starts with {@code functionNamePrefix} (those functions are listed
     * only under this catalog's attaches, and hidden from the main catalog's).
     *
     * @param name the catalog name used in {@code ATTACH '<name>' ...}
     * @param implementationVersion the advertised/resolved implementation version
     * @param dataVersion the advertised {@code data_version_spec} and resolved data version
     * @param schemaComment the comment on the catalog's single {@code main} schema
     * @param functionNamePrefix the function-name prefix this catalog owns
     */
    public record ExtraCatalog(String name, String implementationVersion, String dataVersion,
                               String schemaComment, String functionNamePrefix) {}

    private final Map<String, ExtraCatalog> extraCatalogs = new LinkedHashMap<>();

    /**
     * Register an auxiliary catalog served next to the main one.
     *
     * @param catalog the catalog descriptor
     * @return this builder
     */
    public Worker registerExtraCatalog(ExtraCatalog catalog) {
        extraCatalogs.put(catalog.name(), catalog);
        return this;
    }

    /**
     * The auxiliary catalogs registered via {@link #registerExtraCatalog}.
     *
     * @return the catalogs keyed by name, in registration order
     */
    public Map<String, ExtraCatalog> extraCatalogs() { return extraCatalogs; }

    private final Map<String, List<CatalogTable>> extraCatalogTables = new LinkedHashMap<>();

    /**
     * Register a catalog table owned by an auxiliary catalog. Such tables are
     * enumerated only under that catalog's attaches (and never appear in the
     * main catalog's listings). The scan functions they reference should be
     * registered under the catalog's {@code functionNamePrefix} so they are
     * likewise owned by the catalog.
     *
     * @param catalogName the owning auxiliary catalog name
     * @param t the catalog table
     * @return this builder
     */
    public Worker registerExtraCatalogTable(String catalogName, CatalogTable t) {
        extraCatalogTables.computeIfAbsent(catalogName, k -> new ArrayList<>()).add(t);
        return this;
    }

    /**
     * Catalog tables owned by auxiliary catalogs, keyed by catalog name.
     *
     * @return the extra-catalog tables keyed by catalog name
     */
    public Map<String, List<CatalogTable>> extraCatalogTables() { return extraCatalogTables; }

    /**
     * Register a scalar function, callable from SQL and enumerated through
     * {@code catalog_schema_contents_functions}.
     *
     * @param fn the scalar function to register
     * @return this builder
     */
    public Worker registerScalar(ScalarFunction fn) {
        scalars.add(fn);
        return this;
    }

    /**
     * Register a table function, callable from SQL and enumerated through
     * {@code catalog_schema_contents_functions}.
     *
     * @param fn the table function to register
     * @return this builder
     */
    public Worker registerTable(TableFunction fn) {
        tables.add(fn);
        return this;
    }

    /**
     * Register an aggregate function, callable from SQL and enumerated
     * through {@code catalog_schema_contents_functions}.
     *
     * @param fn the aggregate function to register
     * @return this builder
     */
    public Worker registerAggregate(AggregateFunction<?> fn) {
        aggregates.add(fn);
        return this;
    }

    /**
     * Register a table-in-out function (consumes an input relation, streams
     * an output relation), enumerated through
     * {@code catalog_schema_contents_functions}.
     *
     * @param fn the table-in-out function to register
     * @return this builder
     */
    public Worker registerTableInOut(TableInOutFunction fn) {
        tableInOuts.add(fn);
        return this;
    }

    /**
     * Register several scalar functions; equivalent to calling
     * {@link #registerScalar(ScalarFunction)} for each.
     *
     * @param fns the scalar functions to register
     * @return this builder
     */
    public Worker registerScalars(Iterable<? extends ScalarFunction> fns) {
        for (ScalarFunction f : fns) scalars.add(f);
        return this;
    }

    /**
     * Register several table functions; equivalent to calling
     * {@link #registerTable(TableFunction)} for each.
     *
     * @param fns the table functions to register
     * @return this builder
     */
    public Worker registerTables(Iterable<? extends TableFunction> fns) {
        for (TableFunction f : fns) tables.add(f);
        return this;
    }

    /**
     * Register several aggregate functions; equivalent to calling
     * {@link #registerAggregate(AggregateFunction)} for each.
     *
     * @param fns the aggregate functions to register
     * @return this builder
     */
    public Worker registerAggregates(Iterable<? extends AggregateFunction<?>> fns) {
        for (AggregateFunction<?> f : fns) aggregates.add(f);
        return this;
    }

    /**
     * Register a table-buffering (Sink+Source) function: DuckDB sinks the
     * full input through {@code table_buffering_process}/{@code _combine}
     * before the finalize stream sources results back out.
     *
     * @param fn the table-buffering function to register
     * @return this builder
     */
    public Worker registerTableBuffering(farm.query.vgi.buffering.TableBufferingFunction fn) {
        bufferingFns.add(fn);
        return this;
    }

    /**
     * Register several table-buffering functions; equivalent to calling
     * {@link #registerTableBuffering} for each.
     *
     * @param fns the table-buffering functions to register
     * @return this builder
     */
    public Worker registerTableBufferings(Iterable<? extends farm.query.vgi.buffering.TableBufferingFunction> fns) {
        for (var f : fns) bufferingFns.add(f);
        return this;
    }

    /**
     * Table-buffering functions registered via {@link #registerTableBuffering}.
     *
     * @return the registered table-buffering functions, in registration order
     */
    public List<farm.query.vgi.buffering.TableBufferingFunction> bufferingFunctions() {
        return bufferingFns;
    }

    /**
     * Register several table-in-out functions; equivalent to calling
     * {@link #registerTableInOut(TableInOutFunction)} for each.
     *
     * @param fns the table-in-out functions to register
     * @return this builder
     */
    public Worker registerTableInOuts(Iterable<? extends TableInOutFunction> fns) {
        for (TableInOutFunction f : fns) tableInOuts.add(f);
        return this;
    }

    /**
     * Advertise custom session settings in the {@code catalog_attach} result.
     * DuckDB registers each as a {@code SET}-able option whose current value
     * is forwarded to the worker on every bind.
     *
     * @param specs the setting specs to advertise
     * @return this builder
     */
    public Worker settings(SettingSpec... specs) {
        for (SettingSpec s : specs) settings.add(s);
        return this;
    }

    /**
     * Advertise secret types in the {@code catalog_attach} result. DuckDB
     * registers each so {@code CREATE SECRET} of that type resolves against
     * this catalog, and matching secrets flow to the worker on bind.
     *
     * @param specs the secret-type specs to advertise
     * @return this builder
     */
    public Worker secretTypes(SecretTypeSpec... specs) {
        for (SecretTypeSpec s : specs) secretTypes.add(s);
        return this;
    }

    /**
     * Secret types advertised at attach time.
     *
     * @return the registered secret-type specs, in registration order
     */
    public List<SecretTypeSpec> secretTypeSpecs() { return secretTypes; }

    /**
     * Declare the options this worker accepts in DuckDB's
     * {@code ATTACH ... (key value, ...)} clause. Unknown options are
     * rejected client-side; accepted values arrive in the attach request.
     *
     * @param specs the ATTACH-time option specs to accept
     * @return this builder
     */
    public Worker attachOptions(AttachOptionSpec... specs) {
        for (AttachOptionSpec s : specs) attachOptions.add(s);
        return this;
    }

    /**
     * ATTACH-time options declared via {@link #attachOptions(AttachOptionSpec...)}.
     *
     * @return a copy of the registered attach-option specs
     */
    public List<AttachOptionSpec> attachOptionSpecs() { return List.copyOf(attachOptions); }

    /**
     * Register a view, enumerated through {@code catalog_schema_contents_views};
     * its SQL text is expanded by DuckDB at query time.
     *
     * @param v the view to register
     * @return this builder
     */
    public Worker registerView(View v) {
        views.add(v);
        return this;
    }

    /**
     * Register several views; equivalent to calling
     * {@link #registerView(View)} for each.
     *
     * @param vs the views to register
     * @return this builder
     */
    public Worker registerViews(Iterable<? extends View> vs) {
        for (View v : vs) views.add(v);
        return this;
    }

    /**
     * Views enumerated through {@code catalog_schema_contents_views}.
     *
     * @return the registered views, in registration order
     */
    public List<View> views() { return views; }

    /**
     * Register several catalog tables; equivalent to calling
     * {@link #registerCatalogTable(CatalogTable)} for each.
     *
     * @param ts the catalog tables to register
     * @return this builder
     */
    public Worker registerCatalogTables(Iterable<? extends CatalogTable> ts) {
        for (CatalogTable t : ts) catalogTables.add(t);
        return this;
    }

    /**
     * Catalog name surfaced through {@code catalog_catalogs()}.
     *
     * @return the catalog name (default {@code "vgi"})
     */
    public String catalogName() { return catalogName; }

    /**
     * Catalog comment surfaced through {@code catalog_catalogs()}.
     *
     * @return the catalog comment (default empty, never {@code null})
     */
    public String catalogComment() { return catalogComment; }

    /**
     * Catalog tags surfaced through {@code catalog_catalogs()}.
     *
     * @return the catalog tag key/value pairs, in insertion order
     */
    public Map<String, String> catalogTags() { return catalogTags; }

    /**
     * Schema DuckDB selects by default after ATTACH.
     *
     * @return the default schema name (default {@code "main"})
     */
    public String defaultSchema() { return defaultSchema; }

    /**
     * Session settings advertised at attach time.
     *
     * @return the registered setting specs, in registration order
     */
    public List<SettingSpec> settingSpecs() { return settings; }

    /**
     * @param sealOpaqueData HTTP-only AEAD sealing of attach / transaction
     *        opaque data. Disabled for stdio / AF_UNIX, where OS process
     *        ownership already enforces caller identity. When the caller
     *        configured {@link #opaqueDataKey(byte[])}, that key is also
     *        passed down so multi-replica deployments share a key.
     */
    private RpcServer buildServer(boolean sealOpaqueData) {
        RpcServer server = new RpcServer(VgiService.class,
                new VgiServiceImpl(this, scalars, tables, tableInOuts, aggregates,
                        sealOpaqueData, sealOpaqueData ? opaqueDataKey : null));
        server.setProtocolVersion(VGI_PROTOCOL_VERSION);
        return server;
    }

    /** Block on stdin/stdout serving requests until the transport closes. */
    public void runStdio() {
        try (StdioTransport t = new StdioTransport()) {
            buildServer(false).serve(t);
        }
    }

    /**
     * Block accepting AF_UNIX connections on {@code socketPath}, dispatching
     * each client on a virtual thread, implementing the VGI launcher protocol:
     * the worker prints {@code UNIX:<path>\n} to stdout once the listener is
     * bound, then serves until the process is killed or the idle watchdog fires.
     *
     * <p>{@code idleTimeoutMs <= 0} means "no timeout" — the worker runs
     * until the launcher SIGTERMs it.
     *
     * @param socketPath    filesystem path the AF_UNIX listener binds to
     * @param idleTimeoutMs idle watchdog in milliseconds; {@code <= 0} disables it
     * @throws IOException if the socket cannot be bound or served
     */
    public void runUnixSocket(Path socketPath, long idleTimeoutMs) throws IOException {
        // idleTimeoutMs <= 0 → never time out (legacy behaviour). The
        // launcher passes --idle-timeout 300 by default; the watchdog inside
        // UnixSocketTransport.serveForever closes the listener when active
        // connections stay at zero past that boundary, letting the JVM exit.
        UnixSocketTransport.serveForever(socketPath, buildServer(false), idleTimeoutMs);
    }

    /**
     * Run as an HTTP server bound to {@code host}/{@code port}, blocking until shutdown.
     *
     * @param host bind host
     * @param port bind port ({@code 0} for an ephemeral port)
     * @throws Exception if the server fails to start or serve
     */
    public void runHttp(String host, int port) throws Exception {
        runHttp(HttpServer.Config.builder().host(host).port(port).build());
    }

    /**
     * Canonical CLI dispatcher used by worker {@code main} methods. Parses
     * the four flags every VGI worker accepts and runs the matching transport:
     * <ul>
     *   <li>{@code --unix <path>}: AF_UNIX socket (launcher protocol)
     *   <li>{@code --http} with optional {@code --host}, {@code --port}: HTTP
     *   <li>{@code --idle-timeout <seconds>}: passed to {@code runUnixSocket}
     *   <li>(default): stdio
     * </ul>
     * Also honours {@code VGI_WORKER_STDERR}: redirects {@link System#err} to
     * the named file (in append mode) before any other work, so
     * launcher-mode crashes — where the launcher dup2's {@code /dev/null}
     * over fd 2 — remain inspectable.
     *
     * <p>Unknown args exit with status 2; transport-run failures with 1.
     *
     * @param args  argv as received by {@code main}
     * @param httpCustomizer  applied to the {@code HttpServer.Config.Builder}
     *        seeded with {@code host} and {@code port}; use {@code b -> b}
     *        for defaults, or to layer in JWT / TLS / byte-limit config
     */
    public void runFromArgs(String[] args,
                             java.util.function.UnaryOperator<HttpServer.Config.Builder> httpCustomizer) {
        String stderrPath = System.getenv("VGI_WORKER_STDERR");
        if (stderrPath != null && !stderrPath.isEmpty()) {
            try {
                System.setErr(new java.io.PrintStream(
                        new java.io.FileOutputStream(stderrPath, true), true));
            } catch (Exception ignore) {}
        }
        boolean http = false;
        String host = "127.0.0.1";
        int port = 0;
        String unixSocket = null;
        long idleTimeoutMs = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--http" -> http = true;
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--unix" -> unixSocket = args[++i];
                case "--idle-timeout" -> idleTimeoutMs =
                        (long) (Double.parseDouble(args[++i]) * 1000.0);
                default -> { System.err.println("unknown arg: " + args[i]); System.exit(2); }
            }
        }
        try {
            if (unixSocket != null) {
                runUnixSocket(Path.of(unixSocket), idleTimeoutMs);
            } else if (http) {
                HttpServer.Config.Builder b = HttpServer.Config.builder().host(host).port(port);
                if (httpCustomizer != null) b = httpCustomizer.apply(b);
                runHttp(b.build());
            } else {
                runStdio();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** Convenience overload — equivalent to
     *  {@code runFromArgs(args, b -> b)}.
     *
     * @param args argv as received by {@code main}
     */
    public void runFromArgs(String[] args) {
        runFromArgs(args, b -> b);
    }

    /** HTTP variant that accepts a fully-built config (prefix, authenticator,
     *  TLS, byte limits, …). Used by workers that wire OAuth/JWT or other
     *  production knobs from environment variables.
     *
     *  <p>On SIGTERM the shutdown hook fires, calling {@link HttpServer#stop()}.
     *  Jetty awaits in-flight requests up to its configured stop timeout
     *  (15 s by default; see {@code HttpServer}'s {@code setStopTimeout}) and
     *  then forcibly closes any stragglers.
     *
     * @param config fully-built HTTP server configuration
     * @throws Exception if the server fails to start or serve
     */
    public void runHttp(HttpServer.Config config) throws Exception {
        org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Worker.class);
        HttpServer http = new HttpServer(buildServer(true), config);
        http.start();
        System.out.println("PORT:" + http.port());
        System.out.flush();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("SIGTERM received — stopping HTTP server (graceful, up to 15s)");
            long t0 = System.currentTimeMillis();
            try {
                http.stop();
                log.info("HTTP server stopped after {} ms", System.currentTimeMillis() - t0);
            } catch (Exception e) {
                log.warn("HTTP server stop failed after {} ms: {}",
                        System.currentTimeMillis() - t0, e.toString());
            }
        }, "vgi-http-shutdown"));
        http.join();
    }
}
