// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Blended ("UNNEST-style") table-in-out: positional args ARE per-row input columns.
 *
 * <p>A {@code RowTransformFunction} collapses the classic either/or between a
 * standard table function (literal args only) and a table-in-out function (an
 * explicit {@code TABLE} subquery arg). Its <strong>positional</strong>
 * {@link farm.query.vgi.function.ArgSpec}s declare its per-row input columns —
 * real typed args, NO synthetic {@code TABLE} placeholder — so ONE registration
 * serves every call shape:
 *
 * <pre>
 *   f(52, 13)                       -- literal   -&gt; one input row
 *   FROM t, f(t.x, t.y)             -- columns   -&gt; streaming input
 *   SELECT ... FROM t, LATERAL f(t.x, t.y)
 * </pre>
 *
 * <p><strong>Contract.</strong>
 * <ul>
 *   <li>Positional args are the input columns; they arrive on the exchange's
 *       input batch (by declared name for fixed args, {@code col0..colN-1} for
 *       varargs — read them positionally). They are NOT on the wire arguments.</li>
 *   <li>Named args stay bind-time scalars on {@code params.arguments().named()}.</li>
 *   <li>Map-shaped, per-row: 1-&gt;1, 1-&gt;N, 1-&gt;0 all work. There is
 *       <em>no finalize</em> — {@link #hasFinalize()} is final-false here
 *       (DuckDB forbids {@code FinalExecute} under correlated LATERAL, one of
 *       the call shapes blended must serve). Accumulating functions use a
 *       classic {@code TABLE}-input table-in-out or a
 *       {@link farm.query.vgi.buffering.TableBufferingFunction}.</li>
 *   <li>A positional const arg must not be declared (in the column form DuckDB
 *       sweeps a constant into the input subquery; in the literal form it is
 *       indistinguishable from an input column). Use a named arg for optional
 *       config.</li>
 * </ul>
 *
 * <p>Implementing this interface (not a metadata flag) IS the blended signal —
 * a per-arg or metadata flag could be forgotten on one of N same-named
 * overloads; the type cannot. The wire {@code FunctionInfo.input_from_args} is
 * derived from it, and the C++ extension reads that to enter the in-out
 * registration branch with real-typed args and drive the literal single-row
 * scan-mode. Mirrors vgi-python's {@code RowTransformFunction}.
 */
public interface RowTransformFunction extends TableInOutFunction {

    /**
     * Per-batch metadata key carrying per-output-row provenance for the batched
     * correlated LATERAL operator: a base64-encoded raw little-endian
     * {@code int32[]} where element {@code i} is the input-row index that
     * produced output row {@code i}. Absent metadata = identity 1-&gt;1 map
     * (the extension assumes it and requires output rows == input rows).
     */
    String PARENT_ROW_KEY = "vgi_rpc.parent_row#b64";

    /** Blended is map-shaped by construction; a finalize is structurally impossible.
     *  @return always {@code false}. */
    @Override default boolean hasFinalize() { return false; }

    /**
     * Fold per-output-row provenance into an emit metadata map, for a blended
     * map whose output row count differs from its input's (1-&gt;N fan-out /
     * 1-&gt;0 filter). {@code parentRows[i]} is the 0-based index (into the
     * input batch) of the row that produced output row {@code i}; the array
     * length MUST equal the emitted batch's row count. Encoded as a raw
     * little-endian {@code int32[]} (NOT Arrow IPC), base64, under
     * {@link #PARENT_ROW_KEY} — mirroring vgi-python's
     * {@code _merge_parent_rows}. An empty array skips the key entirely
     * (nothing to map).
     *
     * @param parentRows one input-row index per emitted output row.
     * @param outputRows the emitted batch's row count (validated against
     *     {@code parentRows.length}).
     * @param metadata metadata to merge into, or {@code null}.
     * @return the merged metadata map (never {@code null} when a key was added).
     */
    static Map<String, String> parentRows(int[] parentRows, int outputRows,
                                            Map<String, String> metadata) {
        if (parentRows == null) return metadata;
        if (parentRows.length != outputRows) {
            throw new IllegalStateException(
                    "parentRows length " + parentRows.length + " != output rows " + outputRows
                    + "; parent_rows must carry exactly one input-row index per emitted output row");
        }
        if (outputRows == 0) return metadata;
        ByteBuffer raw = ByteBuffer.allocate(parentRows.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int idx : parentRows) raw.putInt(idx);
        Map<String, String> merged = metadata == null
                ? new LinkedHashMap<>() : new HashMap<>(metadata);
        merged.put(PARENT_ROW_KEY, Base64.getEncoder().encodeToString(raw.array()));
        return merged;
    }
}
