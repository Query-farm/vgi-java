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

    private String catalogName = "vgi";
    private String catalogComment = "";
    private final Map<String, String> catalogTags = new LinkedHashMap<>();
    private String defaultSchema = "main";
    private String implementationVersion;
    private String dataVersionSpec;
    private final List<ScalarFunction> scalars = new ArrayList<>();
    private final List<TableFunction> tables = new ArrayList<>();
    private final List<TableInOutFunction> tableInOuts = new ArrayList<>();
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

    public List<Macro> macros() { return macros; }

    private final List<CatalogTable> catalogTables = new ArrayList<>();

    public Worker registerCatalogTable(CatalogTable t) {
        catalogTables.add(t);
        return this;
    }

    public List<CatalogTable> catalogTables() { return catalogTables; }

    private Worker() {}

    public static Worker builder() { return new Worker(); }

    public Worker catalogName(String name) { this.catalogName = name; return this; }
    public Worker catalogComment(String comment) { this.catalogComment = comment; return this; }
    public Worker catalogTags(Map<String, String> tags) { this.catalogTags.putAll(tags); return this; }
    public Worker implementationVersion(String v) { this.implementationVersion = v; return this; }
    public Worker dataVersionSpec(String v) { this.dataVersionSpec = v; return this; }
    public String implementationVersion() { return implementationVersion; }
    public String dataVersionSpec() { return dataVersionSpec; }
    public Worker defaultSchema(String schema) { this.defaultSchema = schema; return this; }

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

    public List<View> views() { return views; }

    public String catalogName() { return catalogName; }
    public String catalogComment() { return catalogComment; }
    public Map<String, String> catalogTags() { return catalogTags; }
    public String defaultSchema() { return defaultSchema; }
    public List<SettingSpec> settingSpecs() { return settings; }

    private RpcServer buildServer() {
        return new RpcServer(VgiService.class,
                new VgiServiceImpl(this, scalars, tables, tableInOuts, aggregates));
    }

    /** Block on stdin/stdout serving requests until the transport closes. */
    public void runStdio() {
        try (StdioTransport t = new StdioTransport()) {
            buildServer().serve(t);
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
        // Note: idleTimeoutMs is currently accepted but not enforced. The
        // launcher SIGTERMs the worker on test-runner exit, so the test suite
        // doesn't depend on self-exit semantics. Implementing it correctly
        // requires accept-loop instrumentation (track active connection
        // count, treat "zero for N seconds" as the trigger) — TODO once
        // long-running deployments need it.
        UnixSocketTransport.serveForever(socketPath, buildServer());
    }

    public void runHttp(String host, int port) throws Exception {
        runHttp(HttpServer.Config.builder().host(host).port(port).build());
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
        HttpServer http = new HttpServer(buildServer(), config);
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
