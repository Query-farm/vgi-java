// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.Nullable;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import java.util.List;

/**
 * Wire shape for the {@code table_function_plan} RPC reply.
 *
 * <p>A scan plan divides the work into named, independently redeemable splits.
 * Naming the work rather than describing it is what makes a retry safe: "these
 * three files at version 47" survives being re-run, "rows 0-999 of whatever this
 * returns now" does not — and a distributed engine will retry.</p>
 *
 * <p>An EMPTY split list is legal and means "no work": a fully-pruned scan reaches
 * it, and the client must produce an empty result rather than an error.</p>
 *
 * @param splits one serialised {@code ScanSplit} per unit of work, in emission order
 * @param nextCursors continuation cursors; normally 0 or 1. More than one means
 *        parallel enumeration, which is only sound if the cursors partition the
 *        remaining enumeration disjointly — no split reachable from two cursors
 * @param executionId identifier for this scan, echoed on every split init
 * @param initOpaqueData opaque per-init state threaded into each split's stream
 * @param maxWorkers normative cap on splits in flight at once, or {@code null}
 * @param estimatedTotalSplits estimate of the total split count, or {@code null}
 * @param estimatedTotalRows whole-scan row estimate for CBO, or {@code null}
 * @param estimatedTotalBytes whole-scan byte estimate, or {@code null}
 * @param catalogVersion the catalog counter this plan is pinned to
 * @param scope which consistency anchor the tokens bind: {@code catalog} or
 *        {@code transaction}
 * @param locations a hoisted host list splits index into, which keeps a large
 *        plan off the coordinator heap
 * @param partitioning serialised partition transforms. NOT derivable from a
 *        split's partition values — {@code country=US} does not say whether
 *        partitions are {@code identity(country)} or {@code bucket(16, user_id)}.
 *        Report nothing here unless every split really is single-valued
 * @param sortOrder ordering WITHIN each split, never a global claim across
 *        splits: concatenating K non-contiguous sorted runs is not sorted
 * @param cacheMaxAgeSeconds how long this plan stays reusable
 * @param startPosition what the worker actually started from
 * @param endPosition the data frontier resolved at plan time — checkpoint it and
 *        pass it back as the next {@code startPosition}
 */
public record PlanResponse(
        List<byte[]> splits,
        @Nullable List<byte[]> nextCursors,
        @Nullable byte[] executionId,
        @Nullable byte[] initOpaqueData,
        @Nullable Long maxWorkers,
        @Nullable Long estimatedTotalSplits,
        @Nullable Long estimatedTotalRows,
        @Nullable Long estimatedTotalBytes,
        @Nullable Long catalogVersion,
        String scope,
        @Nullable List<String> locations,
        List<byte[]> partitioning,
        List<byte[]> sortOrder,
        @Nullable Long cacheMaxAgeSeconds,
        byte[] startPosition,
        byte[] endPosition) implements ArrowSerializableRecord {

    /** A plan with no splits: legal, and means the scan has no work to do.
     *
     * @return an empty plan at catalog scope
     */
    public static PlanResponse empty() {
        return of(List.of());
    }

    /** A plan carrying the given serialised splits, with everything else at its
     * default. The long-form constructor exists for the wire; this is what a
     * worker calls.
     *
     * @param splits one serialised {@code ScanSplit} per unit of work
     * @return a catalog-scoped plan over those splits
     */
    public static PlanResponse of(List<byte[]> splits) {
        return new PlanResponse(
                splits, List.of(), null, new byte[0], null, null, null, null, null,
                "catalog", null, List.of(), List.of(), null, new byte[0], new byte[0]);
    }
}
