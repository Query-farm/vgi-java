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
 *        later, per tick, and prune WITHIN each split. On a continuation call
 *        (non-empty {@link #cursor}) this already includes {@code
 *        refined_filters}' conjunctive narrowing, merged in — an author sees
 *        one unified filter set for THIS call rather than having to combine
 *        two wire fields themselves
 * @param projectionIds the columns the scan actually reads, or {@code null}
 * @param cursor a place in the ENUMERATION of splits — NOT a place in the data.
 *        A cursor lives for one plan call; a position is checkpointed and must
 *        survive restarts, upgrades and key rotation. Empty on the first call
 * @param minSplits the parallelism FLOOR: a small but expensive table still
 *        needs one reader per thread, which a byte target alone would not give
 *        it. {@code null} when the client has no opinion
 * @param targetSplitBytes the primary sizing lever, since every engine is
 *        byte-driven. {@code null} when the client has no opinion
 * @param maxSplitsPerResponse pagination cap for THIS call — Trino's
 *        {@code ConnectorSplitSource.getNextBatch(maxSize)}, not a sizing
 *        hint. An author who ignores it may return more splits than asked;
 *        the framework does not truncate on their behalf. {@code null} when
 *        the client set no cap
 * @param filtersComplete {@code false} means the client may still narrow
 *        {@code pushdownFilters} further on a later continuation call (via
 *        {@code refined_filters}) — a function that wants to hold back
 *        splits until the filter set stabilizes can check this. {@code true}
 *        (the common case: the client already collected everything, e.g. a
 *        Trino dynamic filter it awaited before planning at all) means what
 *        this call carries is final
 */
public record PlanRequest(
        PushdownFilters pushdownFilters,
        int[] projectionIds,
        byte[] cursor,
        Long minSplits,
        Long targetSplitBytes,
        Long maxSplitsPerResponse,
        boolean filtersComplete) {

    /** A first-page plan call with no pushdown and no sizing request.
     *
     * @return an empty request
     */
    public static PlanRequest empty() {
        return new PlanRequest(null, null, new byte[0], null, null, null, true);
    }
}
