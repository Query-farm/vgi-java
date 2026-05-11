// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi;

import farm.query.vgi.aggregate.AggregateFunction;
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
    private final List<View> views = new ArrayList<>();

    /**
     * A SQL view exposed in the catalog. {@code schema} is the schema name
     * (matches one of {@link #defaultSchema()} or any registered schema);
     * {@code definition} is a SQL query string evaluated by DuckDB.
     */
    public record View(String schema, String name, String definition,
                        String comment, Map<String, String> tags) {
        public View(String schema, String name, String definition, String comment) {
            this(schema, name, definition, comment, Map.of());
        }
    }

    public enum MacroType { SCALAR, TABLE }

    /**
     * A SQL macro exposed in the catalog. {@code parameterDefaults} is an
     * optional ordered map of parameter-name → SQL expression (used when the
     * macro has named-with-default parameters).
     */
    public record Macro(String schema, String name, MacroType macroType,
                         List<String> parameters,
                         @SuppressWarnings("rawtypes") Map<String, String> parameterDefaults,
                         String definition, String comment, Map<String, String> tags) {
        public Macro(String schema, String name, MacroType macroType,
                      List<String> parameters, String definition, String comment) {
            this(schema, name, macroType, parameters, Map.of(), definition, comment, Map.of());
        }

        public Macro(String schema, String name, MacroType macroType,
                      List<String> parameters, Map<String, String> parameterDefaults,
                      String definition, String comment) {
            this(schema, name, macroType, parameters, parameterDefaults, definition, comment, Map.of());
        }
    }

    private final List<Macro> macros = new ArrayList<>();

    public Worker registerMacro(Macro m) {
        macros.add(m);
        return this;
    }

    public List<Macro> macros() { return macros; }

    /**
     * A catalog table. {@code columns} is the table's Arrow schema serialized
     * as IPC bytes. {@code scanFunctionName} (with {@code scanFunctionArgs})
     * inlines the scan function so the C++ extension can skip
     * {@code catalog_table_scan_function_get}; if {@code null}, the worker
     * must implement that RPC manually.
     */
    public record CatalogTable(
            String schema,
            String name,
            byte[] columns,
            String comment,
            Map<String, String> tags,
            String scanFunctionName,
            List<Object> scanFunctionPositional,
            Map<String, Object> scanFunctionNamed,
            Long cardinalityEstimate,
            Long cardinalityMax,
            boolean inlineCardinality,
            boolean inlineScanFunction,
            List<List<Integer>> primaryKey,
            List<List<Integer>> uniqueConstraints,
            List<String> checkConstraints,
            List<ForeignKey> foreignKeys) {

        /** Foreign-key constraint declaration. Wire shape uses column NAMES
         *  (not indices) for both sides — matches the vgi-go fkSchema. */
        public record ForeignKey(
                List<String> fkColumns,
                List<String> pkColumns,
                String referencedSchema,
                String referencedTable) {}

        /** Backward-compat ctor — no constraints. */
        public CatalogTable(String schema, String name, byte[] columns, String comment,
                              Map<String, String> tags, String scanFunctionName,
                              List<Object> scanFunctionPositional, Map<String, Object> scanFunctionNamed,
                              Long cardinalityEstimate, Long cardinalityMax,
                              boolean inlineCardinality, boolean inlineScanFunction) {
            this(schema, name, columns, comment, tags, scanFunctionName, scanFunctionPositional,
                    scanFunctionNamed, cardinalityEstimate, cardinalityMax,
                    inlineCardinality, inlineScanFunction,
                    List.of(), List.of(), List.of(), List.of());
        }

        public static CatalogTable functionBacked(
                String schema, String name, byte[] columns, String comment,
                String scanFunction) {
            return new CatalogTable(schema, name, columns, comment, Map.of(),
                    scanFunction, List.of(), Map.of(), null, null, false, true);
        }

        public CatalogTable withCardinality(long estimate, long max) {
            return new CatalogTable(schema, name, columns, comment, tags,
                    scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                    estimate, max, true, inlineScanFunction,
                    primaryKey, uniqueConstraints, checkConstraints, foreignKeys);
        }

        /** Same backing function but skip the {@code TableInfo.scan_function}
         * inline so the C++ extension fires {@code catalog_table_scan_function_get}. */
        public CatalogTable withRpcScanFunction() {
            return new CatalogTable(schema, name, columns, comment, tags,
                    scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                    cardinalityEstimate, cardinalityMax, inlineCardinality, false,
                    primaryKey, uniqueConstraints, checkConstraints, foreignKeys);
        }

        /** Attach PK/UNIQUE/CHECK/FK constraints to this table. */
        public CatalogTable withConstraints(List<List<Integer>> pk,
                                              List<List<Integer>> unique,
                                              List<String> check,
                                              List<ForeignKey> fks) {
            return new CatalogTable(schema, name, columns, comment, tags,
                    scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                    cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                    pk == null ? List.of() : pk,
                    unique == null ? List.of() : unique,
                    check == null ? List.of() : check,
                    fks == null ? List.of() : fks);
        }
    }

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

    public Worker settings(SettingSpec... specs) {
        for (SettingSpec s : specs) settings.add(s);
        return this;
    }

    public Worker secretTypes(SecretTypeSpec... specs) {
        for (SecretTypeSpec s : specs) secretTypes.add(s);
        return this;
    }

    public List<SecretTypeSpec> secretTypeSpecs() { return secretTypes; }

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
    public List<TableFunction> tableFunctions() { return tables; }
    public List<ScalarFunction> scalarFunctions() { return scalars; }
    public List<AggregateFunction<?>> aggregateFunctions() { return aggregates; }
    public List<TableInOutFunction> tableInOutFunctions() { return tableInOuts; }

    /** Block on stdin/stdout serving requests until the transport closes. */
    public void runStdio() {
        VgiServiceImpl impl = new VgiServiceImpl(this, scalars, tables, tableInOuts, aggregates);
        RpcServer server = new RpcServer(VgiService.class, impl);
        try (StdioTransport t = new StdioTransport()) {
            server.serve(t);
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
        VgiServiceImpl impl = new VgiServiceImpl(this, scalars, tables, tableInOuts, aggregates);
        RpcServer server = new RpcServer(VgiService.class, impl);
        // Note: idleTimeoutMs is currently accepted but not enforced. The
        // launcher SIGTERMs the worker on test-runner exit, so the test suite
        // doesn't depend on self-exit semantics. Implementing it correctly
        // requires accept-loop instrumentation (track active connection
        // count, treat "zero for N seconds" as the trigger) — TODO once
        // long-running deployments need it.
        UnixSocketTransport.serveForever(socketPath, server);
    }

    public void runHttp(String host, int port) throws Exception {
        VgiServiceImpl impl = new VgiServiceImpl(this, scalars, tables, tableInOuts, aggregates);
        RpcServer server = new RpcServer(VgiService.class, impl);
        HttpServer http = new HttpServer(server, HttpServer.Config.builder().host(host).port(port).build());
        http.start();
        System.out.println("PORT:" + http.port());
        System.out.flush();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { http.stop(); } catch (Exception ignore) {}
        }));
        http.join();
    }
}
