// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the SQLite-backed {@link FunctionStorage} for both the in-process
 * ({@code :memory:}) and local-file tiers: the append log's monotonic cursor +
 * scan semantics, batch key/value round-trips, execution_clear, and that the
 * per-connection pragmas match the other SDKs.
 */
class SqliteFunctionStorageTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] u) {
        return u == null ? null : new String(u, StandardCharsets.UTF_8);
    }

    @Test
    void appendLogMonotonicCursorAndScan() {
        try (SqliteFunctionStorage s = new SqliteFunctionStorage(":memory:")) {
            byte[] exec = b("exec-log");
            byte[] ns = b("buf");
            byte[] k = b("k");
            long id1 = s.stateAppend(exec, ns, k, b("a"));
            long id2 = s.stateAppend(exec, ns, k, b("b"));
            long id3 = s.stateAppend(exec, ns, k, b("c"));
            assertTrue(id1 < id2 && id2 < id3, "ids are monotonic");

            List<FunctionStorage.LogEntry> all = s.stateLogScan(exec, ns, k, -1, 0);
            assertEquals(3, all.size());
            assertEquals("a", str(all.get(0).value()));
            assertEquals("c", str(all.get(2).value()));
            // resume after id1 → b, c
            assertEquals(2, s.stateLogScan(exec, ns, k, id1, 0).size());
            // limit caps the page
            assertEquals(2, s.stateLogScan(exec, ns, k, -1, 2).size());
            // key isolation
            assertEquals(0, s.stateLogScan(exec, ns, b("other"), -1, 0).size());
        }
    }

    @Test
    void batchKeyValueRoundTripAndDelete() {
        try (SqliteFunctionStorage s = new SqliteFunctionStorage(":memory:")) {
            byte[] scope = b("exec-kv");
            byte[] ns = b("agg");
            s.statePutMany(scope, ns, List.of(
                    new FunctionStorage.KV(b("k1"), b("v1")),
                    new FunctionStorage.KV(b("k2"), b("v2"))));
            List<byte[]> got = s.stateGetMany(scope, ns, List.of(b("k1"), b("k2"), b("missing")));
            assertEquals("v1", str(got.get(0)));
            assertEquals("v2", str(got.get(1)));
            assertNull(got.get(2));

            s.stateDelete(scope, ns, List.of(b("k1")));
            assertNull(s.stateGet(scope, ns, b("k1")));
            assertEquals("v2", str(s.stateGet(scope, ns, b("k2"))));
        }
    }

    @Test
    void isolatedByScopeAndNamespace() {
        try (SqliteFunctionStorage s = new SqliteFunctionStorage(":memory:")) {
            s.statePut(b("scopeA"), b("ns"), b("k"), b("a"));
            s.statePut(b("scopeB"), b("ns"), b("k"), b("b"));
            s.statePut(b("scopeA"), b("ns2"), b("k"), b("c"));
            assertEquals("a", str(s.stateGet(b("scopeA"), b("ns"), b("k"))));
            assertEquals("b", str(s.stateGet(b("scopeB"), b("ns"), b("k"))));
            assertEquals("c", str(s.stateGet(b("scopeA"), b("ns2"), b("k"))));
        }
    }

    @Test
    void executionClearWipesStateAndLogForScope() {
        try (SqliteFunctionStorage s = new SqliteFunctionStorage(":memory:")) {
            byte[] exec = b("exec-clear");
            s.statePut(exec, b("agg"), b("k"), b("v"));
            s.stateAppend(exec, b("buf"), b("lk"), b("a"));
            int deleted = s.executionClear(exec);
            assertEquals(2, deleted);
            assertNull(s.stateGet(exec, b("agg"), b("k")));
            assertEquals(0, s.stateLogScan(exec, b("buf"), b("lk"), -1, 0).size());
        }
    }

    @Test
    void fileTierPersistsAcrossConnections(@TempDir Path tmp) {
        String path = tmp.resolve("fs.db").toString();
        byte[] exec = b("exec-file");
        byte[] ns = b("ns");
        try (SqliteFunctionStorage s = new SqliteFunctionStorage(path)) {
            s.statePut(exec, ns, b("k"), b("durable"));
        }
        // Reopen: file-backed state survives a fresh connection.
        try (SqliteFunctionStorage s2 = new SqliteFunctionStorage(path)) {
            assertEquals("durable", str(s2.stateGet(exec, ns, b("k"))));
        }
    }

    @Test
    void pragmasMatchTheOtherSdks(@TempDir Path tmp) throws Exception {
        String url = "jdbc:sqlite:" + tmp.resolve("p.db") + "?" + SqliteFunctionStorage.SQLITE_PARAMS;
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            assertEquals("wal", scalarStr(st, "PRAGMA journal_mode"));
            assertEquals(1, scalarInt(st, "PRAGMA synchronous")); // NORMAL
            assertEquals(2, scalarInt(st, "PRAGMA temp_store")); // MEMORY
            assertEquals(-65536, scalarInt(st, "PRAGMA cache_size"));
            assertTrue(scalarInt(st, "PRAGMA busy_timeout") >= 30000);
        }
    }

    private static String scalarStr(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static int scalarInt(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
