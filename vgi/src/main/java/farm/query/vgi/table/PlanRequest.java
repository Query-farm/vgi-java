// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

import farm.query.vgi.pushdown.PushdownFilters;

/**
 * The inputs a {@link TableFunction#plan} call receives beyond its bind
 * parameters: the pushdown it may use to emit fewer splits, and the place in the
 * enumeration it is resuming from.
 *
 * @param pushdownFilters the static filters this scan carries, or {@code null}.
 *        A plan is built from STATIC filters only — join-key values are not
 *        known when this fires, so they cannot prune the split SET; they arrive
 *        later, per tick, and prune WITHIN each split
 * @param projectionIds the columns the scan actually reads, or {@code null}
 * @param cursor a place in the ENUMERATION of splits — NOT a place in the data.
 *        A cursor lives for one plan call; a position is checkpointed and must
 *        survive restarts, upgrades and key rotation. Empty on the first call
 * @param minSplits the parallelism FLOOR: a small but expensive table still
 *        needs one reader per thread, which a byte target alone would not give
 *        it. {@code null} when the client has no opinion
 * @param targetSplitBytes the primary sizing lever, since every engine is
 *        byte-driven. {@code null} when the client has no opinion
 */
public record PlanRequest(
        PushdownFilters pushdownFilters,
        int[] projectionIds,
        byte[] cursor,
        Long minSplits,
        Long targetSplitBytes) {

    /** A first-page plan call with no pushdown and no sizing request.
     *
     * @return an empty request
     */
    public static PlanRequest empty() {
        return new PlanRequest(null, null, new byte[0], null, null);
    }
}
