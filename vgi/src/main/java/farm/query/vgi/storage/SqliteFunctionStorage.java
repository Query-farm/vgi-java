// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * SQLite-backed {@link FunctionStorage} for the in-process ({@code :memory:})
 * and local cross-process (file) tiers.
 *
 * <p>Schema and pragmas mirror vgi-go / vgi-python / vgi-typescript: a
 * {@code function_state} key/value table, a {@code function_state_log} append
 * log whose {@code AUTOINCREMENT} id is the resumable scan cursor (same as
 * vgi-go's {@code state_log}), a {@code function_counter} table of atomic int64
 * counters, and a {@code work_queue} FIFO. The state tables are addressed by
 * {@code (scope_id, ns, key)}. The constructor self-heals an older on-disk DB:
 * tables carrying the pre-unification HTTP-idempotency columns (or obsolete
 * tables eliminated by the unified schema) are dropped and recreated — all of
 * this is ephemeral in-progress worker state, so dropping is safe.
 *
 * <p>Concurrency follows vgi-go's model: a single long-lived connection per
 * process serializes operations (this class synchronizes every method), and WAL
 * + {@code busy_timeout} let multiple worker processes sharing a file see each
 * other's rows. A single held connection is also what keeps a {@code :memory:}
 * database alive for the worker's lifetime (an in-memory DB exists only as long
 * as a connection to it is open).
 */
public final class SqliteFunctionStorage implements FunctionStorage {

    /**
     * Per-connection pragmas, identical to {@code AggregateStateStore.SQLITE_PARAMS}
     * and the other SDKs: WAL for cross-process concurrency, synchronous=NORMAL,
     * a 30s busy_timeout, temp_store=MEMORY, and a 64 MiB page cache. (WAL is a
     * no-op for {@code :memory:}, which SQLite keeps in "memory" journal mode.)
     */
    public static final String SQLITE_PARAMS =
            "journal_mode=WAL&busy_timeout=30000&synchronous=NORMAL&temp_store=MEMORY&cache_size=-65536";

    private final Connection conn;

    /**
     * Opens (and initializes the schema of) a SQLite database.
     *
     * @param path the database path, or {@code ":memory:"} for the in-process tier
     */
    public SqliteFunctionStorage(String path) {
        String url = "jdbc:sqlite:" + path + "?" + SQLITE_PARAMS;
        try {
            this.conn = DriverManager.getConnection(url);
            try (java.sql.Statement st = conn.createStatement()) {
                // Self-heal: this tier carries none of the DO's HTTP-idempotency
                // machinery, so a table left over with the old columns (written
                // by a pre-unification SDK build) is dropped and recreated minimal.
                dropIfStaleColumn(st, "function_state", "last_attempt_id");
                dropIfStaleColumn(st, "function_state_log", "attempt_id");
                dropIfStaleColumn(st, "work_queue", "created_at");
                for (String dead : new String[] {
                        "global_state_storage", "worker_state", "invocation_registry", "init_storage"}) {
                    st.execute("DROP TABLE IF EXISTS " + dead);
                }
                st.execute("CREATE TABLE IF NOT EXISTS function_state ("
                        + "  scope_id BLOB NOT NULL,"
                        + "  ns BLOB NOT NULL,"
                        + "  key BLOB NOT NULL,"
                        + "  value BLOB NOT NULL,"
                        + "  PRIMARY KEY(scope_id, ns, key))");
                st.execute("CREATE TABLE IF NOT EXISTS function_state_log ("
                        + "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        + "  scope_id BLOB NOT NULL,"
                        + "  ns BLOB NOT NULL,"
                        + "  key BLOB NOT NULL,"
                        + "  value BLOB NOT NULL)");
                st.execute("CREATE INDEX IF NOT EXISTS function_state_log_cursor "
                        + "  ON function_state_log(scope_id, ns, key, id)");
                st.execute("CREATE TABLE IF NOT EXISTS function_counter ("
                        + "  scope_id BLOB NOT NULL,"
                        + "  ns BLOB NOT NULL,"
                        + "  key BLOB NOT NULL,"
                        + "  n INTEGER NOT NULL,"
                        + "  PRIMARY KEY(scope_id, ns, key))");
                st.execute("CREATE TABLE IF NOT EXISTS work_queue ("
                        + "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        + "  execution_id BLOB NOT NULL,"
                        + "  work_item BLOB NOT NULL)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_work_queue_execution "
                        + "  ON work_queue(execution_id, id)");
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage: open/init at " + path, e);
        }
    }

    private static void dropIfStaleColumn(java.sql.Statement st, String table, String staleCol)
            throws SQLException {
        boolean stale = false;
        try (ResultSet rs = st.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                if (staleCol.equals(rs.getString("name"))) {
                    stale = true;
                    break;
                }
            }
        }
        if (stale) {
            st.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /** Resolve the per-user, per-machine file path for the local cross-process tier. */
    static String defaultDbPath() {
        String env = System.getenv("VGI_WORKER_SQLITE_PATH");
        if (env != null && !env.isEmpty()) return env;
        // SDK-private directory ("vgi-java", not "vgi"): the local SQLite file
        // is a private implementation detail, and a shared "vgi/vgi_storage.db"
        // would collide with vgi-typescript's default path, whose function_state
        // schema carries a NOT NULL last_attempt_id column this client doesn't
        // populate. The cross-SDK contract is the DO wire protocol, not this file.
        String home = System.getProperty("user.home", System.getenv("HOME"));
        String os = System.getProperty("os.name", "").toLowerCase();
        Path base;
        if (os.contains("mac")) {
            base = Paths.get(home, "Library", "Application Support", "vgi-java");
        } else {
            String xdg = System.getenv("XDG_STATE_HOME");
            base = (xdg != null && !xdg.isEmpty())
                    ? Paths.get(xdg, "vgi-java")
                    : Paths.get(home, ".local", "state", "vgi-java");
        }
        try {
            Files.createDirectories(base);
        } catch (Exception e) {
            throw new RuntimeException("SqliteFunctionStorage: cannot create dir " + base, e);
        }
        return base.resolve("vgi_storage.db").toString();
    }

    @Override
    public synchronized long stateAppend(byte[] scopeId, byte[] ns, byte[] key, byte[] value) {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO function_state_log(scope_id, ns, key, value) VALUES(?,?,?,?)",
                java.sql.Statement.RETURN_GENERATED_KEYS)) {
            ps.setBytes(1, scopeId);
            ps.setBytes(2, ns);
            ps.setBytes(3, key);
            ps.setBytes(4, value);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateAppend", e);
        }
    }

    @Override
    public synchronized List<LogEntry> stateLogScan(byte[] scopeId, byte[] ns, byte[] key, long afterId, int limit) {
        String sql = "SELECT id, value FROM function_state_log "
                + "WHERE scope_id=? AND ns=? AND key=? AND id>? ORDER BY id"
                + (limit > 0 ? " LIMIT " + limit : "");
        List<LogEntry> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setBytes(1, scopeId);
            ps.setBytes(2, ns);
            ps.setBytes(3, key);
            ps.setLong(4, afterId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new LogEntry(rs.getLong(1), rs.getBytes(2)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateLogScan", e);
        }
        return out;
    }

    @Override
    public synchronized List<byte[]> stateGetMany(byte[] scopeId, byte[] ns, List<byte[]> keys) {
        List<byte[]> out = new ArrayList<>(keys.size());
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT value FROM function_state WHERE scope_id=? AND ns=? AND key=?")) {
            for (byte[] key : keys) {
                ps.setBytes(1, scopeId);
                ps.setBytes(2, ns);
                ps.setBytes(3, key);
                try (ResultSet rs = ps.executeQuery()) {
                    out.add(rs.next() ? rs.getBytes(1) : null);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateGetMany", e);
        }
        return out;
    }

    @Override
    public synchronized void statePutMany(byte[] scopeId, byte[] ns, List<KV> items) {
        if (items.isEmpty()) return;
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT OR REPLACE INTO function_state(scope_id, ns, key, value) VALUES(?,?,?,?)")) {
                for (KV kv : items) {
                    ps.setBytes(1, scopeId);
                    ps.setBytes(2, ns);
                    ps.setBytes(3, kv.key());
                    ps.setBytes(4, kv.value());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new RuntimeException("SqliteFunctionStorage.statePutMany", e);
        } finally {
            restoreAutoCommit();
        }
    }

    @Override
    public synchronized int stateDelete(byte[] scopeId, byte[] ns, List<byte[]> keys) {
        if (keys.isEmpty()) return 0;
        int deleted = 0;
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM function_state WHERE scope_id=? AND ns=? AND key=?")) {
                for (byte[] key : keys) {
                    ps.setBytes(1, scopeId);
                    ps.setBytes(2, ns);
                    ps.setBytes(3, key);
                    ps.addBatch();
                }
                for (int n : ps.executeBatch()) {
                    if (n > 0) deleted += n;
                }
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new RuntimeException("SqliteFunctionStorage.stateDelete", e);
        } finally {
            restoreAutoCommit();
        }
        return deleted;
    }

    @Override
    public synchronized int stateDeleteRange(byte[] scopeId, byte[] ns, byte[] start, byte[] end) {
        StringBuilder sql = new StringBuilder("DELETE FROM function_state WHERE scope_id=? AND ns=?");
        if (start != null) sql.append(" AND key>=?");
        if (end != null) sql.append(" AND key<?");
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int p = 1;
            ps.setBytes(p++, scopeId);
            ps.setBytes(p++, ns);
            if (start != null) ps.setBytes(p++, start);
            if (end != null) ps.setBytes(p++, end);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateDeleteRange", e);
        }
    }

    @Override
    public synchronized List<KV> stateScan(byte[] scopeId, byte[] ns, byte[] start, byte[] end,
                                           boolean reverse, int limit) {
        // BLOB comparison in SQLite is memcmp, which is exactly the unsigned-lex
        // key order the contract requires.
        StringBuilder sql = new StringBuilder(
                "SELECT key, value FROM function_state WHERE scope_id=? AND ns=?");
        if (start != null) sql.append(" AND key>=?");
        if (end != null) sql.append(" AND key<?");
        sql.append(" ORDER BY key ").append(reverse ? "DESC" : "ASC").append(" LIMIT ?");
        List<KV> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int p = 1;
            ps.setBytes(p++, scopeId);
            ps.setBytes(p++, ns);
            if (start != null) ps.setBytes(p++, start);
            if (end != null) ps.setBytes(p++, end);
            ps.setInt(p, limit > 0 ? limit : -1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new KV(rs.getBytes(1), rs.getBytes(2)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateScan", e);
        }
        return out;
    }

    @Override
    public synchronized List<KV> stateDrain(byte[] scopeId, byte[] ns) {
        // DELETE..RETURNING reads and removes in one write statement, so two
        // processes sharing the file can never drain the same rows (a
        // SELECT-then-DELETE under a deferred transaction could). RETURNING
        // row order is unspecified — sort to the contract's key order here.
        List<KV> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM function_state WHERE scope_id=? AND ns=? RETURNING key, value")) {
            ps.setBytes(1, scopeId);
            ps.setBytes(2, ns);
            ps.execute();
            try (ResultSet rs = ps.getResultSet()) {
                while (rs != null && rs.next()) out.add(new KV(rs.getBytes(1), rs.getBytes(2)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateDrain", e);
        }
        out.sort((a, b) -> java.util.Arrays.compareUnsigned(a.key(), b.key()));
        return out;
    }

    // --- Atomic int64 counters (function_counter) ---
    // No idempotency layer: a local single-connection backend has no retries.

    @Override
    public synchronized long stateCounterGet(byte[] scopeId, byte[] ns, byte[] key) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT n FROM function_counter WHERE scope_id=? AND ns=? AND key=?")) {
            ps.setBytes(1, scopeId);
            ps.setBytes(2, ns);
            ps.setBytes(3, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateCounterGet", e);
        }
    }

    @Override
    public synchronized long stateCounterAdd(byte[] scopeId, byte[] ns, byte[] key, long delta) {
        // Single-statement upsert keeps the read-add-return atomic even across
        // processes sharing the file (the write lock serializes the statement).
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO function_counter(scope_id, ns, key, n) VALUES(?,?,?,?) "
                        + "ON CONFLICT(scope_id, ns, key) DO UPDATE SET n = n + excluded.n "
                        + "RETURNING n")) {
            ps.setBytes(1, scopeId);
            ps.setBytes(2, ns);
            ps.setBytes(3, key);
            ps.setLong(4, delta);
            ps.execute();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateCounterAdd", e);
        }
    }

    @Override
    public synchronized void stateCounterSet(byte[] scopeId, byte[] ns, byte[] key, long value) {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO function_counter(scope_id, ns, key, n) VALUES(?,?,?,?) "
                        + "ON CONFLICT(scope_id, ns, key) DO UPDATE SET n = excluded.n")) {
            ps.setBytes(1, scopeId);
            ps.setBytes(2, ns);
            ps.setBytes(3, key);
            ps.setLong(4, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateCounterSet", e);
        }
    }

    @Override
    public synchronized void stateCounterDelete(byte[] scopeId, byte[] ns, byte[] key) {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM function_counter WHERE scope_id=? AND ns=? AND key=?")) {
            ps.setBytes(1, scopeId);
            ps.setBytes(2, ns);
            ps.setBytes(3, key);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.stateCounterDelete", e);
        }
    }

    // --- Work queue (FIFO, destructive pop) ---

    @Override
    public synchronized int queuePush(byte[] executionId, List<byte[]> items) {
        if (items.isEmpty()) return 0;
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO work_queue(execution_id, work_item) VALUES(?,?)")) {
                for (byte[] item : items) {
                    ps.setBytes(1, executionId);
                    ps.setBytes(2, item);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new RuntimeException("SqliteFunctionStorage.queuePush", e);
        } finally {
            restoreAutoCommit();
        }
        return items.size();
    }

    @Override
    public synchronized byte[] queuePop(byte[] executionId) {
        // Single-statement claim: two processes sharing the file can never pop
        // the same item (a SELECT-then-DELETE could).
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM work_queue WHERE id = ("
                        + "SELECT id FROM work_queue WHERE execution_id=? ORDER BY id ASC LIMIT 1) "
                        + "RETURNING work_item")) {
            ps.setBytes(1, executionId);
            ps.execute();
            try (ResultSet rs = ps.getResultSet()) {
                return (rs != null && rs.next()) ? rs.getBytes(1) : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.queuePop", e);
        }
    }

    @Override
    public synchronized int queueClear(byte[] executionId) {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM work_queue WHERE execution_id=?")) {
            ps.setBytes(1, executionId);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage.queueClear", e);
        }
    }

    @Override
    public synchronized int executionClear(byte[] scopeId) {
        int deleted = 0;
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM function_state WHERE scope_id=?")) {
                ps.setBytes(1, scopeId);
                deleted += ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM function_state_log WHERE scope_id=?")) {
                ps.setBytes(1, scopeId);
                deleted += ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM function_counter WHERE scope_id=?")) {
                ps.setBytes(1, scopeId);
                deleted += ps.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new RuntimeException("SqliteFunctionStorage.executionClear", e);
        } finally {
            restoreAutoCommit();
        }
        return deleted;
    }

    @Override
    public synchronized void close() {
        try {
            conn.close();
        } catch (SQLException ignored) {
            // closing on shutdown — nothing to recover.
        }
    }

    private void rollbackQuietly() {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // best-effort
        }
    }

    private void restoreAutoCommit() {
        try {
            conn.setAutoCommit(true);
        } catch (SQLException ignored) {
            // best-effort
        }
    }
}
