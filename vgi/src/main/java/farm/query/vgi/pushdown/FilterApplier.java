// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Helper for table-function fixtures that opt into filter pushdown
 * ({@code FunctionMetadata.filterPushdown=true}, {@code autoApplyFilters=true}).
 *
 * <p>Decoded once per init via {@link #from} and reused across emits — each
 * {@link #apply} evaluates the pre-parsed filter AST against the batch and
 * returns a compacted batch (closing the original on row drop).</p>
 */
public final class FilterApplier {

    private final byte[] filterBytes;
    private final List<byte[]> joinKeysIpc;
    private transient PushdownFilters cached;

    /**
     * Create an applier over the init-time pushdown payload; decoding is deferred
     * to first {@link #apply}.
     *
     * @param filterBytes the pushdown-filter IPC bytes, or {@code null} when none were pushed
     * @param joinKeysIpc the {@code InitRequest.join_keys} IPC batches, or {@code null} for none
     * @return a reusable applier
     */
    public static FilterApplier from(byte[] filterBytes, List<byte[]> joinKeysIpc) {
        return new FilterApplier(filterBytes, joinKeysIpc);
    }

    private FilterApplier(byte[] filterBytes, List<byte[]> joinKeysIpc) {
        this.filterBytes = filterBytes;
        this.joinKeysIpc = joinKeysIpc == null ? List.of() : joinKeysIpc;
    }

    private PushdownFilters filters() {
        if (cached == null) {
            cached = filterBytes == null
                    ? PushdownFilters.empty()
                    : PushdownFiltersDecoder.decode(filterBytes, joinKeysIpc);
        }
        return cached;
    }

    /**
     * Compact {@code src} to only rows that pass the parsed filters. Returns
     * {@code src} unchanged when no filters were pushed; closes {@code src}
     * and returns a new root when rows are dropped.
     *
     * @param src the batch to filter; ownership is transferred (it is closed when rows are dropped)
     * @return {@code src} itself when nothing is dropped, otherwise a new compacted root
     */
    public VectorSchemaRoot apply(VectorSchemaRoot src) {
        PushdownFilters pf = filters();
        if (pf.filters().isEmpty()) return src;
        boolean[] mask = pf.evaluate(src);
        int kept = 0;
        for (boolean b : mask) if (b) kept++;
        if (kept == src.getRowCount()) return src;
        VectorSchemaRoot dst = VectorSchemaRoot.create(src.getSchema(), Allocators.root());
        dst.allocateNew();
        int dstIdx = 0;
        for (int i = 0; i < mask.length; i++) {
            if (!mask[i]) continue;
            for (int c = 0; c < dst.getFieldVectors().size(); c++) {
                FieldVector dv = dst.getVector(c);
                FieldVector svv = src.getVector(c);
                dv.copyFromSafe(i, dstIdx, svv);
            }
            dstIdx++;
        }
        dst.setRowCount(kept);
        src.close();
        return dst;
    }
}
