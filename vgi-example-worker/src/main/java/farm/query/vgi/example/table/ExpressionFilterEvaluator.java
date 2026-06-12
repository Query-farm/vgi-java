// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.haybarn.DuckDBConnection;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgirpc.wire.Allocators;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

/**
 * Applies pushed expression filters (spatial {@code &&},
 * {@code st_intersects_extent}, {@code list_contains}, ...) to an emitted batch
 * by evaluating their rendered SQL against an embedded Haybarn engine — the
 * Java analogue of vgi-python's {@code vgi._duckdb} expression-filter evaluator.
 *
 * <p>Each worker thread gets its own lazily-opened in-memory connection with the
 * spatial extension loaded (geometry predicates need it); the connection lives
 * for the process. A batch is bridged into the engine via the Arrow C Data
 * interface, the combined predicate is evaluated to a per-row boolean mask, and
 * the batch is compacted via {@link FilterApplier#compact}.
 */
public final class ExpressionFilterEvaluator {

    private ExpressionFilterEvaluator() {}

    private static final ThreadLocal<Connection> CONN = new ThreadLocal<>();
    private static final java.util.concurrent.atomic.AtomicLong VIEW_SEQ =
            new java.util.concurrent.atomic.AtomicLong();

    private static Connection connection() throws Exception {
        Connection c = CONN.get();
        if (c != null && !c.isClosed()) return c;
        c = DriverManager.getConnection("jdbc:haybarn::memory:");
        try (Statement st = c.createStatement()) {
            // Geometry predicates (&&, st_intersects_extent) need spatial; install
            // is a no-op once cached. Non-spatial predicates work without it, so a
            // failure here (offline) is non-fatal.
            try {
                st.execute("INSTALL spatial FROM core");
                st.execute("LOAD spatial");
            } catch (Exception ignore) {
                // spatial unavailable — list/string predicates still evaluate.
            }
        }
        CONN.set(c);
        return c;
    }

    /**
     * Compact {@code src} to the rows satisfying every predicate in
     * {@code predicates} (conjoined). Returns {@code src} unchanged when there
     * are no predicates; otherwise closes {@code src} on row drop (ownership
     * transfer), consistent with {@link FilterApplier#apply}.
     *
     * @param src        the batch to filter; closed when rows are dropped
     * @param predicates rendered SQL boolean predicates over {@code src}'s columns
     * @return {@code src} itself when nothing is dropped, otherwise a new compacted root
     */
    public static VectorSchemaRoot apply(VectorSchemaRoot src, List<String> predicates) {
        if (predicates == null || predicates.isEmpty() || src.getRowCount() == 0) return src;
        String combined = "(" + String.join(") AND (", predicates) + ")";
        String view = "_vgi_eval_" + VIEW_SEQ.incrementAndGet();

        byte[] ipc;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ArrowStreamWriter w = new ArrowStreamWriter(src, null, Channels.newChannel(bos))) {
            w.start();
            w.writeBatch();
            w.end();
            ipc = bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("expression-filter: serialise batch failed", e);
        }

        boolean[] mask = new boolean[src.getRowCount()];
        try {
            Connection c = connection();
            try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), Allocators.root());
                    ArrowArrayStream stream = ArrowArrayStream.allocateNew(Allocators.root())) {
                Data.exportArrayStream(Allocators.root(), reader, stream);
                c.unwrap(DuckDBConnection.class).registerArrowStream(view, stream);
                int i = 0;
                try (Statement st = c.createStatement();
                        ResultSet rs = st.executeQuery(
                                "SELECT (" + combined + ")::BOOLEAN AS r FROM " + view)) {
                    while (rs.next() && i < mask.length) {
                        boolean v = rs.getBoolean(1);
                        mask[i++] = v && !rs.wasNull();
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("expression-filter: evaluate '" + combined + "' failed", e);
        }
        return FilterApplier.compact(src, mask);
    }
}
