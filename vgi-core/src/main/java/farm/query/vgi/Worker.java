// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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
 * metadata, then call {@link #runStdio()} or {@link #runHttp(String)}.
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

    public Worker registerMacro(Macro m) {
        macros.add(m);
        return this;
    }

    public Worker registerMacros(Iterable<? extends Macro> ms) {
        for (Macro m : ms) macros.add(m);
        return this;
    }

    public List<Macro> macros() { return macros; }

    private final List<CatalogTable> catalogTables = new ArrayList<>();
    private final Map<String, List<farm.query.vgi.catalog.ScanBranch>> multiBranchTables =
            new LinkedHashMap<>();

    public Worker registerCatalogTable(CatalogTable t) {
        catalogTables.add(t);
        return this;
    }

    public List<CatalogTable> catalogTables() { return catalogTables; }

    /**
     * Register a multi-branch table: a catalog table whose scan is the
     * {@code UNION_ALL} of {@code branches}. The table is enumerated normally;
     * its scan resolves through {@code catalog_table_scan_branches_get}. Pass
     * an empty branch list to exercise the C++ loud-fail path. The stub's
     * inline scan-function (if any) is dropped so the branches RPC drives.
     */
    public Worker registerMultiBranchTable(CatalogTable stub,
            List<farm.query.vgi.catalog.ScanBranch> branches) {
        catalogTables.add(stub.withRpcScanFunction());
        multiBranchTables.put(stub.schema() + "." + stub.name(),
                branches == null ? List.of() : List.copyOf(branches));
        return this;
    }

    /** Branches for a multi-branch table, or {@code null} if not multi-branch. */
    public List<farm.query.vgi.catalog.ScanBranch> multiBranchTable(String schema, String name) {
        return multiBranchTables.get(schema + "." + name);
    }

    public Map<String, List<farm.query.vgi.catalog.ScanBranch>> multiBranchTables() {
        return multiBranchTables;
    }

    private Worker() {}

    public static Worker builder() { return new Worker(); }

    public Worker catalogName(String name) { this.catalogName = name; return this; }
    public Worker catalogComment(String comment) { this.catalogComment = comment; return this; }
    public Worker catalogTags(Map<String, String> tags) { this.catalogTags.putAll(tags); return this; }
    public Worker implementationVersion(String v) { this.implementationVersion = v; return this; }
    public Worker dataVersionSpec(String v) { this.dataVersionSpec = v; return this; }
    public String implementationVersion() { return implementationVersion; }
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
     *  Pass newest-first. */
    public Worker releases(CatalogDataVersionRelease... rs) {
        for (CatalogDataVersionRelease r : rs) releases.add(r);
        return this;
    }
    public List<CatalogDataVersionRelease> releases() { return List.copyOf(releases); }
    public Worker sourceUrl(String url) { this.sourceUrl = url; return this; }
    public String sourceUrl() { return sourceUrl; }
    public Worker defaultSchema(String schema) { this.defaultSchema = schema; return this; }

    /** Per-schema comment surfaced via {@code catalog_schemas} /
     *  {@code catalog_schema_get}. Default comment for the default schema is
     *  "Default schema"; any auxiliary schema without an entry gets an empty
     *  comment. */
    public Worker schemaComment(String schema, String comment) {
        schemaComments.put(schema, comment == null ? "" : comment);
        return this;
    }

    public Map<String, String> schemaComments() { return schemaComments; }

    public Worker registerScalar(ScalarFunction fn) {
        scalars.add(fn);
        return this;
    }

    public Worker registerTable(TableFunction fn) {
        tables.add(fn);
        return this;
    }

    public Worker registerAggregate(AggregateFunction<?> fn) {
        aggregates.add(fn);
        return this;
    }

    public Worker registerTableInOut(TableInOutFunction fn) {
        tableInOuts.add(fn);
        return this;
    }

    public Worker registerScalars(Iterable<? extends ScalarFunction> fns) {
        for (ScalarFunction f : fns) scalars.add(f);
        return this;
    }

    public Worker registerTables(Iterable<? extends TableFunction> fns) {
        for (TableFunction f : fns) tables.add(f);
        return this;
    }

    public Worker registerAggregates(Iterable<? extends AggregateFunction<?>> fns) {
        for (AggregateFunction<?> f : fns) aggregates.add(f);
        return this;
    }

    public Worker registerTableBuffering(farm.query.vgi.buffering.TableBufferingFunction fn) {
        bufferingFns.add(fn);
        return this;
    }

    public Worker registerTableBufferings(Iterable<? extends farm.query.vgi.buffering.TableBufferingFunction> fns) {
        for (var f : fns) bufferingFns.add(f);
        return this;
    }

    public List<farm.query.vgi.buffering.TableBufferingFunction> bufferingFunctions() {
        return bufferingFns;
    }

    public Worker registerTableInOuts(Iterable<? extends TableInOutFunction> fns) {
        for (TableInOutFunction f : fns) tableInOuts.add(f);
        return this;
    }

    public Worker settings(SettingSpec... specs) {
        for (SettingSpec s : specs) settings.add(s);
        return this;
    }

    public Worker secretTypes(SecretTypeSpec... specs) {
        for (SecretTypeSpec s : specs) secretTypes.add(s);
        return this;
    }

    public List<SecretTypeSpec> secretTypeSpecs() { return secretTypes; }

    public Worker attachOptions(AttachOptionSpec... specs) {
        for (AttachOptionSpec s : specs) attachOptions.add(s);
        return this;
    }

    public List<AttachOptionSpec> attachOptionSpecs() { return List.copyOf(attachOptions); }

    public Worker registerView(View v) {
        views.add(v);
        return this;
    }

    public Worker registerViews(Iterable<? extends View> vs) {
        for (View v : vs) views.add(v);
        return this;
    }

    public List<View> views() { return views; }

    public Worker registerCatalogTables(Iterable<? extends CatalogTable> ts) {
        for (CatalogTable t : ts) catalogTables.add(t);
        return this;
    }

    public String catalogName() { return catalogName; }
    public String catalogComment() { return catalogComment; }
    public Map<String, String> catalogTags() { return catalogTags; }
    public String defaultSchema() { return defaultSchema; }
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
     * Run as an HTTP server, blocking until {@link Thread#interrupt} or
     * shutdown hook fires. Prints {@code PORT:<n>} to stdout once bound so
     * test wrappers can capture the port.
     *
     * <p>HTTP mode is preferred over subprocess pooling because all DuckDB
     * pool slots converge on a single JVM — aggregate state and TIO execution
     * state are naturally shared across "workers" without a file-backed
     * coordination layer.
     */
    /**
     * Block accepting AF_UNIX connections on {@code socketPath}, dispatching
     * each client on a virtual thread. Mirrors the
     * <a href="../../../../../../../../../Development/vgi/docs/launcher-protocol.md">
     * VGI launcher protocol</a>: the worker prints {@code UNIX:<path>\n} to
     * stdout once the listener is bound, then serves until the process is
     * killed or the idle watchdog fires.
     *
     * <p>{@code idleTimeoutMs <= 0} means "no timeout" — the worker runs
     * until the launcher SIGTERMs it.
     */
    public void runUnixSocket(Path socketPath, long idleTimeoutMs) throws IOException {
        // idleTimeoutMs <= 0 → never time out (legacy behaviour). The
        // launcher passes --idle-timeout 300 by default; the watchdog inside
        // UnixSocketTransport.serveForever closes the listener when active
        // connections stay at zero past that boundary, letting the JVM exit.
        UnixSocketTransport.serveForever(socketPath, buildServer(false), idleTimeoutMs);
    }

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
     *  {@code runFromArgs(args, b -> b)}. */
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
     *  then forcibly closes any stragglers. */
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
