// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.CatalogContentsResponse;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.transport.RpcTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The example worker — the fixture set the cross-language integration suite
 * runs against — advertises {@code catalog_contents}, and its answer is the
 * per-schema RPCs' answers byte for byte, for every kind the fixtures register
 * (scalar, table and aggregate functions, catalog tables, views, scalar and
 * table macros).
 */
final class ExampleCatalogContentsTest {

    /** A client connected to {@code worker} over an in-process pipe pair. */
    private record Pipe(VgiService vgi, RpcConnection connection, Thread server) implements AutoCloseable {
        static Pipe start(Worker worker) throws Exception {
            PipedOutputStream clientOut = new PipedOutputStream();
            PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 22);
            PipedOutputStream serverOut = new PipedOutputStream();
            PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 22);
            RpcServer rpc = worker.rpcServer();
            Thread t = new Thread(() -> rpc.serve(transport(serverIn, serverOut)), "vgi-example-worker");
            t.setDaemon(true);
            t.start();
            RpcConnection connection = new RpcConnection(transport(clientIn, clientOut));
            return new Pipe(connection.proxy(VgiService.class), connection, t);
        }

        private static RpcTransport transport(InputStream in, OutputStream out) {
            return new RpcTransport() {
                @Override public InputStream reader() { return in; }
                @Override public OutputStream writer() { return out; }
                @Override public void close() {
                    try { out.close(); } catch (Exception ignore) { /* best-effort */ }
                    try { in.close(); } catch (Exception ignore) { /* best-effort */ }
                }
            };
        }

        @Override public void close() throws Exception {
            connection.close();
            server.join(5000);
        }
    }

    private static CatalogAttachResult attach(VgiService vgi) {
        return vgi.catalog_attach(CatalogAttachRequest.of("example", null, null, null), null);
    }

    @Test
    void catalogContentsSwitchParsesTheEnvironmentValue() {
        assertTrue(Main.catalogContentsEnabled(null));
        assertTrue(Main.catalogContentsEnabled("1"));
        assertTrue(Main.catalogContentsEnabled("true"));
        for (String off : List.of("0", "false", "OFF", " no ")) {
            assertFalse(Main.catalogContentsEnabled(off), off);
        }
    }

    @Test
    @Timeout(120)
    void exampleWorkerServesItsWholeCatalogByteForByte() throws Exception {
        try (Pipe p = Pipe.start(Main.buildWorker("example", null, null))) {
            VgiService vgi = p.vgi();
            CatalogAttachResult attached = attach(vgi);
            assertTrue(attached.supports_catalog_contents(), "the example worker advertises catalog_contents");
            byte[] handle = attached.attach_opaque_data();

            CatalogContentsResponse response = vgi.catalog_contents(handle, null, null);
            assertEquals(vgi.catalog_version(handle, null, null).version(), response.catalog_version());

            List<byte[]> schemaItems = vgi.catalog_schemas(handle, null, null).items();
            assertEquals(schemaItems.size(), response.schemas().size());
            int[] totals = new int[8];
            for (int i = 0; i < schemaItems.size(); i++) {
                SchemaContents c = response.schemas().get(i);
                assertArrayEquals(schemaItems.get(i), c.schema(), "schema item " + i);
                List<String> path = RecordCodec.deserializeFromBytes(c.schema(), SchemaInfo.class).path();
                assertEquals(path, c.path(), "path equals SchemaInfo.path");
                Map<String, ItemsResponse> perSchema = Map.of(
                        "tables", vgi.catalog_schema_contents_tables(handle, path, null, null),
                        "views", vgi.catalog_schema_contents_views(handle, path, null, null),
                        "scalar_functions",
                        vgi.catalog_schema_contents_functions(handle, path, "SCALAR_FUNCTION", null, null),
                        "aggregate_functions",
                        vgi.catalog_schema_contents_functions(handle, path, "AGGREGATE_FUNCTION", null, null),
                        "table_functions",
                        vgi.catalog_schema_contents_functions(handle, path, "TABLE_FUNCTION", null, null),
                        "scalar_macros", vgi.catalog_schema_contents_macros(handle, path, "SCALAR_MACRO", null, null),
                        "table_macros", vgi.catalog_schema_contents_macros(handle, path, "TABLE_MACRO", null, null),
                        "indexes", vgi.catalog_schema_contents_indexes(handle, path, null, null));
                List<List<byte[]>> bulk = List.of(c.tables(), c.views(), c.scalar_functions(),
                        c.aggregate_functions(), c.table_functions(), c.scalar_macros(), c.table_macros(),
                        c.indexes());
                List<String> kinds = List.of("tables", "views", "scalar_functions", "aggregate_functions",
                        "table_functions", "scalar_macros", "table_macros", "indexes");
                for (int k = 0; k < kinds.size(); k++) {
                    List<byte[]> expected = perSchema.get(kinds.get(k)).items();
                    List<byte[]> actual = bulk.get(k);
                    String what = path + " " + kinds.get(k);
                    assertEquals(expected.size(), actual.size(), what + ": item count");
                    for (int j = 0; j < actual.size(); j++) {
                        assertArrayEquals(expected.get(j), actual.get(j), what + ": item " + j);
                    }
                    totals[k] += actual.size();
                }
            }
            // The fixture set covers every kind but indexes; a zero here would
            // mean the comparison above passed vacuously for that kind.
            for (int k = 0; k < 7; k++) {
                assertTrue(totals[k] > 0, "kind " + k + " has no items anywhere in the example catalog");
            }
        }
    }

    @Test
    @Timeout(120)
    void theFixtureWorkerRevalidatesWithAVersionEtagByDefault() throws Exception {
        Worker w = Main.configureCatalogContents(Main.buildWorker("example", null, null), null, null);
        try (Pipe p = Pipe.start(w)) {
            VgiService vgi = p.vgi();
            byte[] handle = attach(vgi).attach_opaque_data();
            long version = vgi.catalog_version(handle, null, null).version();

            CatalogContentsResponse full = vgi.catalog_contents(handle, null, null);
            assertEquals("gen-" + version, full.etag());
            assertFalse(full.not_modified());
            assertFalse(full.schemas().isEmpty());

            CatalogContentsResponse same = vgi.catalog_contents(handle, full.etag(), null);
            assertTrue(same.not_modified());
            assertTrue(same.schemas().isEmpty());
            assertEquals(full.etag(), same.etag());

            CatalogContentsResponse other = vgi.catalog_contents(handle, "not-the-etag", null);
            assertFalse(other.not_modified());
            assertEquals(full.schemas().size(), other.schemas().size());
        }
    }

    @Test
    @Timeout(120)
    void theFixtureWorkerEtagModes() throws Exception {
        try (Pipe p = Pipe.start(Main.configureCatalogContents(
                Main.buildWorker("example", null, null), null, "content-hash"))) {
            VgiService vgi = p.vgi();
            byte[] handle = attach(vgi).attach_opaque_data();
            CatalogContentsResponse full = vgi.catalog_contents(handle, null, null);
            assertEquals(64, full.etag().length());
            assertEquals(full.etag(), vgi.catalog_contents(handle, null, null).etag(),
                    "the example catalog encodes deterministically");
            assertTrue(vgi.catalog_contents(handle, full.etag(), null).not_modified());
        }
        try (Pipe p = Pipe.start(Main.configureCatalogContents(
                Main.buildWorker("example", null, null), null, "none"))) {
            VgiService vgi = p.vgi();
            byte[] handle = attach(vgi).attach_opaque_data();
            assertNull(vgi.catalog_contents(handle, null, null).etag());
            assertFalse(vgi.catalog_contents(handle, "x", null).not_modified());
        }
        assertThrows(IllegalArgumentException.class, () -> Main.configureCatalogContents(
                Main.buildWorker("example", null, null), null, "bogus"));
    }

    @Test
    @Timeout(120)
    void turningItOffClearsTheAttachFlag() throws Exception {
        try (Pipe p = Pipe.start(Main.buildWorker("example", null, null).supportsCatalogContents(false))) {
            assertFalse(attach(p.vgi()).supports_catalog_contents());
        }
    }
}
