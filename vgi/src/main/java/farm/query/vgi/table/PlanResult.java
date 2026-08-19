// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

import farm.query.vgi.protocol.ScanSplit;
import java.util.List;

/**
 * What a {@link TableFunction#plan} call produces: the splits, plus the few
 * plan-level facts an author can meaningfully set.
 *
 * <p>Deliberately NOT the wire {@code PlanResponse}, which carries sixteen
 * fields — execution ids, opaque data, partitioning transforms, sort orders,
 * positions — that the framework fills in or that no author should have to
 * think about. This is the author-facing subset, mirroring Rust's
 * {@code PlanOutcome} and TypeScript's {@code PlanResult}.</p>
 *
 * <p>Set only {@code payload} on each split. The framework stamps the
 * consistency anchor, the bind fingerprint and — where a signing key exists —
 * the seal, so an author cannot forget the anchor or mis-bind the fingerprint,
 * and never writes crypto.</p>
 *
 * @param splits one entry per unit of work. EMPTY is legal and means "no work":
 *        a fully-pruned scan reaches it, and the client must produce an empty
 *        result rather than an error
 * @param nextCursors continued enumeration. More than one MUST partition the
 *        remaining enumeration disjointly and exhaustively. No client checks
 *        this — a dedup was tried and removed, because it needed a copy of every
 *        token, it compared token bytes and so could never fire on a keyed
 *        worker, and the most a client can do with a duplicate is refuse anyway.
 *        Violating it returns DUPLICATE ROWS, silently
 * @param catalogVersion the snapshot this plan is pinned to, or {@code null} to
 *        use the catalog's live version. It is the anchor every token in this
 *        plan is stamped with and checked against, so naming a version the
 *        catalog will not agree with is how a plan is made to expire
 * @param estimatedTotalSplits estimate of the whole enumeration's split count
 * @param estimatedTotalRows whole-scan row estimate, for CBO
 * @param maxWorkers NORMATIVE cap on redemption concurrency, not advisory
 */
public record PlanResult(
        List<ScanSplit> splits,
        List<byte[]> nextCursors,
        Long catalogVersion,
        Long estimatedTotalSplits,
        Long estimatedTotalRows,
        Long maxWorkers) {

    /** The not-split-capable answer: no splits, so the client scans normally.
     *
     * @return an empty plan
     */
    public static PlanResult none() {
        return new PlanResult(List.of(), List.of(), null, null, null, null);
    }

    /** A finished plan: these splits and no continuation.
     *
     * @param splits one per unit of work
     * @return a plan carrying them
     */
    public static PlanResult of(List<ScanSplit> splits) {
        return new PlanResult(splits, List.of(), null, (long) splits.size(), null, null);
    }

    /** This plan, pinned to a specific catalog version.
     *
     * @param version the snapshot this plan was built against
     * @return a copy carrying that version
     */
    public PlanResult withCatalogVersion(long version) {
        return new PlanResult(splits, nextCursors, version, estimatedTotalSplits,
                estimatedTotalRows, maxWorkers);
    }

    /** This plan, with a continuation cursor so enumeration carries on.
     *
     * @param cursor the worker's own bytes naming where to resume
     * @return a copy carrying that cursor
     */
    public PlanResult withNextCursor(byte[] cursor) {
        return new PlanResult(splits, List.of(cursor), catalogVersion, estimatedTotalSplits,
                estimatedTotalRows, maxWorkers);
    }
}
