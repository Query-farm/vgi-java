// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SQLite-backed cross-process state store for aggregate functions.
 *
 * <p>DuckDB spawns multiple worker subprocesses for parallel aggregation, so
 * partial states accumulated in worker A's heap need to be visible to worker
 * B's combine call. We mirror vgi-go's approach: a single SQLite file shared
 * between all worker processes via OS file locking. Each worker reads its
 * relevant rows, mutates, and writes them back per RPC call.
 *
 * <p>Path resolution (in order):
 * <ol>
 *   <li>{@code VGI_JAVA_AGGREGATE_DB} env var (absolute path)</li>
 *   <li>{@code XDG_STATE_HOME/vgi-java/aggregate_storage.db} on Linux</li>
 *   <li>{@code $HOME/Library/Application Support/vgi-java/aggregate_storage.db} on macOS</li>
 *   <li>{@code $HOME/.local/state/vgi-java/aggregate_storage.db} fallback</li>
 * </ol>
 *
 * <p>Each row holds Java-serialised state bytes. The framework supplies the
 * codec via {@link farm.query.vgi.aggregate.AggregateFunction#serializeState}.
 */
public final class AggregateStateStore {

    private static volatile AggregateStateStore INSTANCE;

    private final String url;
    private volatile boolean initialised;

    private AggregateStateStore(String url) {
        this.url = url;
    }

    public static AggregateStateStore get() {
        AggregateStateStore s = INSTANCE;
        if (s == null) {
            synchronized (AggregateStateStore.class) {
                s = INSTANCE;
                if (s == null) {
                    Path dbPath = resolveDbPath();
                    try {
                        Files.createDirectories(dbPath.getParent());
                    } catch (Exception e) {
                        throw new RuntimeException("AggregateStateStore: cannot create dir " + dbPath.getParent(), e);
                    }
                    String url = "jdbc:sqlite:" + dbPath
                            + "?journal_mode=WAL&busy_timeout=30000&synchronous=NORMAL";
                    s = new AggregateStateStore(url);
                    INSTANCE = s;
                }
            }
        }
        return s;
    }

    private static Path resolveDbPath() {
        String env = System.getenv("VGI_JAVA_AGGREGATE_DB");
        if (env != null && !env.isEmpty()) return Paths.get(env);
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
        return base.resolve("aggregate_storage.db");
    }

    private Connection conn() throws SQLException {
        Connection c = DriverManager.getConnection(url);
        if (!initialised) {
            synchronized (this) {
                if (!initialised) {
                    try (java.sql.Statement st = c.createStatement()) {
                        st.execute("CREATE TABLE IF NOT EXISTS agg_state (" +
                                "  execution_id BLOB NOT NULL," +
                                "  function_name TEXT NOT NULL," +
                                "  group_id INTEGER NOT NULL," +
                                "  state BLOB NOT NULL," +
                                "  PRIMARY KEY(execution_id, function_name, group_id))");
                        st.execute("CREATE TABLE IF NOT EXISTS agg_args (" +
                                "  execution_id BLOB NOT NULL," +
                                "  function_name TEXT NOT NULL," +
                                "  args BLOB NOT NULL," +
                                "  PRIMARY KEY(execution_id, function_name))");
                    }
                    initialised = true;
                }
            }
        }
        return c;
    }

    /** Save bind-time arguments for an execution_id. */
    public void saveArgs(byte[] executionId, String functionName, byte[] argsIpc) {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT OR REPLACE INTO agg_args(execution_id, function_name, args) VALUES(?,?,?)")) {
            ps.setBytes(1, executionId);
            ps.setString(2, functionName);
            ps.setBytes(3, argsIpc);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("AggregateStateStore.saveArgs", e);
        }
    }

    /** Load bind-time arguments for an execution_id. Returns null if absent. */
    public byte[] loadArgs(byte[] executionId, String functionName) {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT args FROM agg_args WHERE execution_id=? AND function_name=?")) {
            ps.setBytes(1, executionId);
            ps.setString(2, functionName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getBytes(1) : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("AggregateStateStore.loadArgs", e);
        }
    }

    /** Load state bytes for the given group_ids. Missing keys are absent from the map. */
    public Map<Long, byte[]> loadStates(byte[] executionId, String functionName, long[] gids) {
        if (gids.length == 0) return new HashMap<>();
        StringBuilder sb = new StringBuilder(
                "SELECT group_id, state FROM agg_state WHERE execution_id=? AND function_name=? AND group_id IN (");
        for (int i = 0; i < gids.length; i++) sb.append(i == 0 ? "?" : ",?");
        sb.append(")");
        Map<Long, byte[]> out = new LinkedHashMap<>();
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sb.toString())) {
            ps.setBytes(1, executionId);
            ps.setString(2, functionName);
            for (int i = 0; i < gids.length; i++) ps.setLong(3 + i, gids[i]);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.put(rs.getLong(1), rs.getBytes(2));
            }
        } catch (SQLException e) {
            throw new RuntimeException("AggregateStateStore.loadStates", e);
        }
        return out;
    }

    /** Upsert state bytes for the given (gid → bytes) entries. */
    public void saveStates(byte[] executionId, String functionName, Map<Long, byte[]> states) {
        if (states.isEmpty()) return;
        try (Connection c = conn()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT OR REPLACE INTO agg_state(execution_id, function_name, group_id, state) " +
                            "VALUES(?,?,?,?)")) {
                for (Map.Entry<Long, byte[]> e : states.entrySet()) {
                    ps.setBytes(1, executionId);
                    ps.setString(2, functionName);
                    ps.setLong(3, e.getKey());
                    ps.setBytes(4, e.getValue());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            c.commit();
        } catch (SQLException e) {
            throw new RuntimeException("AggregateStateStore.saveStates", e);
        }
    }

    /** Delete the given (executionId, function, gid) tuples. */
    public void deleteStates(byte[] executionId, String functionName, long[] gids) {
        if (gids.length == 0) return;
        try (Connection c = conn()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement(
                    "DELETE FROM agg_state WHERE execution_id=? AND function_name=? AND group_id=?")) {
                for (long g : gids) {
                    ps.setBytes(1, executionId);
                    ps.setString(2, functionName);
                    ps.setLong(3, g);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            c.commit();
        } catch (SQLException e) {
            throw new RuntimeException("AggregateStateStore.deleteStates", e);
        }
    }

}
