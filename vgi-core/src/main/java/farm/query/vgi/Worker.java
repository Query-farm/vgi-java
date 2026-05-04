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
    private final List<ScalarFunction> scalars = new ArrayList<>();
    private final List<TableFunction> tables = new ArrayList<>();
    private final List<TableInOutFunction> tableInOuts = new ArrayList<>();
    private final List<AggregateFunction<?>> aggregates = new ArrayList<>();
    private final List<SettingSpec> settings = new ArrayList<>();

    private Worker() {}

    public static Worker builder() { return new Worker(); }

    public Worker catalogName(String name) { this.catalogName = name; return this; }
    public Worker catalogComment(String comment) { this.catalogComment = comment; return this; }
    public Worker catalogTags(Map<String, String> tags) { this.catalogTags.putAll(tags); return this; }
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
