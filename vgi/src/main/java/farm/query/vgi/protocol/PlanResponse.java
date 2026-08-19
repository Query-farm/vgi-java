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
 * @param next_cursors continuation cursors; normally 0 or 1. More than one means
 *        parallel enumeration, which is only sound if the cursors partition the
 *        remaining enumeration disjointly — no split reachable from two cursors.
 *        No client checks this: a dedup was tried and removed (it needed a set
 *        holding a copy of every token, it compared token bytes so it could never
 *        work on a keyed worker where each mint uses a fresh nonce, and the most a
 *        client can do with a duplicate is refuse anyway). Violating it returns
 *        DUPLICATE ROWS, silently
 * @param execution_id identifier for this scan, echoed on every split init
 * @param init_opaque_data opaque per-init state threaded into each split's stream
 * @param max_workers normative cap on splits in flight at once, or {@code null}
 * @param estimated_total_splits estimate of the total split count, or {@code null}
 * @param estimated_total_rows whole-scan row estimate for CBO, or {@code null}
 * @param estimated_total_bytes whole-scan byte estimate, or {@code null}
 * @param catalog_version the catalog counter this plan is pinned to
 * @param scope which consistency anchor the tokens bind: {@code catalog} or
 *        {@code transaction}
 * @param locations a hoisted host list splits index into, which keeps a large
 *        plan off the coordinator heap
 * @param partitioning serialised partition transforms. NOT derivable from a
 *        split's partition values — {@code country=US} does not say whether
 *        partitions are {@code identity(country)} or {@code bucket(16, user_id)}.
 *        Report nothing here unless every split really is single-valued
 * @param sort_order ordering WITHIN each split, never a global claim across
 *        splits: concatenating K non-contiguous sorted runs is not sorted
 * @param cache_max_age_seconds how long this plan stays reusable
 * @param start_position what the worker actually started from
 * @param end_position the data frontier resolved at plan time — checkpoint it and
 *        pass it back as the next {@code start_position}
 */
// Field names are the WIRE names: schema derivation uses each record component
// verbatim, with no camelCase-to-snake_case conversion. Naming these in Java
// style silently produced a response whose columns no client could find — the
// reader looks up by name, finds nothing, and quietly defaults every field.
public record PlanResponse(
        List<byte[]> splits,
        @Nullable List<byte[]> next_cursors,
        @Nullable byte[] execution_id,
        @Nullable byte[] init_opaque_data,
        @Nullable Long max_workers,
        @Nullable Long estimated_total_splits,
        @Nullable Long estimated_total_rows,
        @Nullable Long estimated_total_bytes,
        @Nullable Long catalog_version,
        String scope,
        @Nullable List<String> locations,
        List<byte[]> partitioning,
        List<byte[]> sort_order,
        @Nullable Long cache_max_age_seconds,
        byte[] start_position,
        byte[] end_position) implements ArrowSerializableRecord {

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
