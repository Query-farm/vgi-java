// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper for table-function fixtures that opt into filter pushdown
 * ({@code FunctionMetadata.filterPushdown=true}, {@code autoApplyFilters=true}).
 *
 * <p>Decoded once per init via {@link #from} and reused across emits — each
 * {@link #apply} evaluates the pre-parsed filter AST against the batch and
 * returns a compacted batch (closing the original on row drop).</p>
 */
public final class FilterApplier {

    // Non-final + a no-arg constructor so the HTTP transport's field-based CBOR
    // state serializer can round-trip a FilterApplier held in a producer's
    // StreamState across /init -> /exchange. cached is transient (re-decoded
    // lazily after a round-trip).
    private byte[] filterBytes;
    private List<byte[]> joinKeysIpc;
    // The dynamic-filter deltas a rebuild replays over the init snapshot, compacted
    // by applyDelta, and the live predicate order that replay must restore.
    private List<byte[]> filterDeltas;
    private List<String> predicateOrder;
    private Schema bindOutputSchema;
    private PushdownFiltersDecoder.Capabilities capabilities;
    private transient PushdownFilters cached;

    /** No-arg constructor for state deserialization; {@link #from} is the API. */
    private FilterApplier() {
        this.joinKeysIpc = List.of();
        this.filterDeltas = List.of();
        this.predicateOrder = List.of();
        this.capabilities = PushdownFiltersDecoder.Capabilities.core();
    }

    /**
     * Create an applier without a bind schema only for an empty filter payload.
     *
     * @param filterBytes the pushdown-filter IPC bytes, or {@code null} when none were pushed
     * @param joinKeysIpc the {@code InitRequest.join_keys} IPC batches, or {@code null} for none
     * @return an empty reusable applier
     * @deprecated V2 filter evaluation requires the authoritative unprojected bind output schema
     *     whenever a filter payload is present.
     */
    @Deprecated(forRemoval = true)
    public static FilterApplier from(byte[] filterBytes, List<byte[]> joinKeysIpc) {
        if (filterBytes == null || filterBytes.length == 0) {
            return new FilterApplier(null, joinKeysIpc, null,
                    PushdownFiltersDecoder.Capabilities.core());
        }
        throw new FilterV2Exception(
                "v2 filter evaluation requires the authoritative bind output schema");
    }

    /** Create an applier with the authoritative bind schema and negotiated capabilities. */
    public static FilterApplier from(
            byte[] filterBytes, List<byte[]> joinKeysIpc, Schema bindOutputSchema,
            PushdownFiltersDecoder.Capabilities capabilities) {
        return new FilterApplier(filterBytes, joinKeysIpc, bindOutputSchema, capabilities);
    }

    private FilterApplier(byte[] filterBytes, List<byte[]> joinKeysIpc, Schema bindOutputSchema,
                          PushdownFiltersDecoder.Capabilities capabilities) {
        this.filterBytes = filterBytes;
        this.joinKeysIpc = joinKeysIpc == null ? List.of() : joinKeysIpc;
        this.filterDeltas = List.of();
        this.predicateOrder = List.of();
        this.bindOutputSchema = bindOutputSchema;
        this.capabilities = capabilities == null
                ? PushdownFiltersDecoder.Capabilities.core() : capabilities;
    }

    private PushdownFilters filters() {
        if (cached == null) {
            PushdownFilters rebuilt = filterBytes == null
                    ? PushdownFilters.empty()
                    : PushdownFiltersDecoder.decode(filterBytes, bindOutputSchema,
                            joinKeysIpc, capabilities == null
                                    ? PushdownFiltersDecoder.Capabilities.core() : capabilities);
            if (filterDeltas != null && !filterDeltas.isEmpty()) {
                for (byte[] delta : filterDeltas) rebuilt = rebuilt.applyDelta(delta);
                if (predicateOrder != null) rebuilt = rebuilt.withPredicateOrder(predicateOrder);
            }
            cached = rebuilt;
        }
        return cached;
    }

    /**
     * The current filter state: the init snapshot with every applied delta.
     *
     * @return the parsed filters this applier evaluates
     */
    public PushdownFilters current() {
        return filters();
    }

    /**
     * Validate and atomically apply a tick-time v2 advisory delta.
     *
     * <p>An HTTP stream keeps no parsed state between turns: each turn
     * deserialises this applier and rebuilds its filters from the init snapshot
     * plus the deltas it carries. Keeping <em>every</em> delta made turn
     * {@code k} replay {@code k} of them and grew the continuation token by one
     * delta per tick -- quadratic in the tick count, and a Top-N scan tightens
     * its bound on nearly every tick. So the carried history is compacted to the
     * deltas that installed some predicate's <em>current</em> revision,
     * tombstones included: for each {@code (id, revision)} of the live state, the
     * first delta that carried it. Replaying just those reproduces the same
     * predicates, values and revisions -- an ID's earlier updates are overwritten
     * by its current revision, and later ones were stale and stay stale -- so the
     * history is bounded by the number of predicate IDs, not the number of ticks.
     * Replay cannot always reproduce predicate order (an ID removed and re-added
     * moves to the end), so the live order is recorded and restored.
     *
     * @param delta the delta's Arrow IPC bytes; {@code null} or empty is a no-op
     * @throws FilterV2Exception if the delta is malformed; the state is then unchanged
     */
    public void applyDelta(byte[] delta) {
        if (delta == null || delta.length == 0) return;
        PushdownFilters updated = filters().applyDelta(delta);
        List<byte[]> history = new ArrayList<>(filterDeltas == null ? List.of() : filterDeltas);
        history.add(delta.clone());
        filterDeltas = compact(history, updated);
        List<String> order = new ArrayList<>(updated.predicates().size());
        for (FilterPredicateV2 predicate : updated.predicates()) order.add(predicate.id());
        predicateOrder = order;
        cached = updated;
    }

    /** The deltas of {@code history} that first carried each {@code (id, revision)} of {@code live}. */
    private static List<byte[]> compact(List<byte[]> history, PushdownFilters live) {
        Set<PushdownFiltersDecoder.Revision> wanted = new HashSet<>();
        for (Map.Entry<String, Long> entry : live.revisions().entrySet()) {
            wanted.add(new PushdownFiltersDecoder.Revision(entry.getKey(), entry.getValue()));
        }
        List<byte[]> kept = new ArrayList<>();
        for (byte[] delta : history) {
            boolean needed = false;
            for (PushdownFiltersDecoder.Revision carried : PushdownFiltersDecoder.deltaRevisions(delta)) {
                needed |= wanted.remove(carried);
            }
            if (needed) kept.add(delta);
        }
        return kept;
    }

    /** How many deltas a rebuild of this applier replays. */
    int retainedDeltaCount() {
        return filterDeltas == null ? 0 : filterDeltas.size();
    }

    /**
     * The rendered SQL predicates of any pushed expression filters (spatial
     * {@code &&}, {@code list_contains}, ...). {@link #apply} only handles
     * column filters; expression filters are pass-through there and must be
     * applied separately via the worker's expression evaluator.
     *
     * @return the expression-filter SQL predicates, in order; empty when none
     */
    public List<String> expressionPredicates() {
        return filters().expressionPredicates();
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
        return compact(src, pf.evaluate(src));
    }

    /**
     * Compact {@code src} to only the rows whose {@code mask} entry is {@code true}.
     * Returns {@code src} unchanged when every row is kept; otherwise builds a new
     * root and closes {@code src} (ownership transfer). Shared by the column-filter
     * path here and the worker's expression-filter evaluator.
     *
     * @param src  the batch to compact; closed when rows are dropped
     * @param mask one entry per row — {@code true} keeps the row
     * @return {@code src} itself when nothing is dropped, otherwise a new compacted root
     */
    public static VectorSchemaRoot compact(VectorSchemaRoot src, boolean[] mask) {
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
