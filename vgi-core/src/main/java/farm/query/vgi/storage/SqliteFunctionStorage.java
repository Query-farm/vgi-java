// Copyright 2025-2026 Query.Farm LLC

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
 * {@code function_state} key/value table and a {@code function_state_log}
 * append log whose {@code AUTOINCREMENT} id is the resumable scan cursor (same
 * as vgi-go's {@code state_log}). Both are addressed by {@code (scope_id, ns,
 * key)}.
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

    /** Open at {@code path} ({@code ":memory:"} for the in-process tier). */
    public SqliteFunctionStorage(String path) {
        String url = "jdbc:sqlite:" + path + "?" + SQLITE_PARAMS;
        try {
            this.conn = DriverManager.getConnection(url);
            try (java.sql.Statement st = conn.createStatement()) {
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
            }
        } catch (SQLException e) {
            throw new RuntimeException("SqliteFunctionStorage: open/init at " + path, e);
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
    public synchronized void stateDelete(byte[] scopeId, byte[] ns, List<byte[]> keys) {
        if (keys.isEmpty()) return;
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
                ps.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new RuntimeException("SqliteFunctionStorage.stateDelete", e);
        } finally {
            restoreAutoCommit();
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
