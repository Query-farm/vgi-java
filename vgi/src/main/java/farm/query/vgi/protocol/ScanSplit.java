// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.Nullable;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import java.util.List;

/**
 * One named, independently redeemable unit of scan work.
 *
 * <p>A split <em>names</em> work rather than describing it: "these three files at
 * version 47" survives a retry, "rows 0-999 of whatever this returns now" does
 * not — and a distributed engine will retry. The same split may also be redeemed
 * more than once (recursive CTEs, re-collected DataFrames, task retry) and may be
 * abandoned mid-stream (LIMIT, TopK, an empty join build side); neither is an
 * error.</p>
 *
 * <p>A worker sets {@code payload} and nothing else. The framework stamps
 * {@code token} from it — the consistency anchor, the bind fingerprint and, where
 * a signing key exists, the seal — so an author cannot forget the anchor or
 * mis-bind the fingerprint, and never writes crypto. The client sends the TOKEN
 * back, never the raw payload.</p>
 *
 * @param payload the worker's own opaque bytes naming this unit of work
 * @param token the framework-stamped envelope. Populated by the framework; a
 *        worker must not set it
 * @param estimated_rows row estimate, or {@code null} if unknown
 * @param rows_exact whether {@code estimated_rows} is exact rather than an
 *        estimate — unlocks COUNT(*) from statistics
 * @param estimated_bytes byte estimate. Load-bearing for engines that bin-pack
 *        (DataFusion weight, Trino SplitWeight); {@code null} degrades them to
 *        round-robin by count. A greedily claiming client needs no cost model
 * @param key_value exact one-row transformed key tuple for key grouping
 * @param range_index zero-based range owned by this split
 * @param column_bounds 2-row (min, max) containment/pruning metadata
 * @param partition_bounds deprecated protocol-2 spelling for column bounds
 * @param column_statistics per-column statistics blob for this split
 * @param location_ids indices into {@code PlanResponse.locations} naming hosts
 *        where this split is cheap to read
 * @param start_position exclusive lower bound of this split's range in the data
 * @param end_position inclusive upper bound; {@code null} means UNBOUNDED — a
 *        shard read forever, which a bounded engine must refuse rather than hang
 */
public record ScanSplit(
        byte[] payload,
        byte[] token,
        @Nullable Long estimated_rows,
        boolean rows_exact,
        @Nullable Long estimated_bytes,
        @Nullable byte[] key_value,
        @Nullable Long range_index,
        @Nullable byte[] column_bounds,
        @Nullable byte[] partition_bounds,
        @Nullable byte[] column_statistics,
        @Nullable List<Long> location_ids,
        @Nullable byte[] start_position,
        @Nullable byte[] end_position) implements ArrowSerializableRecord {

    /** Protocol-2 source compatibility while protocol 3 producers migrate. */
    @Deprecated
    public ScanSplit(
            byte[] payload,
            byte[] token,
            @Nullable Long estimated_rows,
            boolean rows_exact,
            @Nullable Long estimated_bytes,
            @Nullable byte[] partition_bounds,
            @Nullable byte[] column_statistics,
            @Nullable List<Long> location_ids,
            @Nullable byte[] start_position,
            @Nullable byte[] end_position) {
        this(payload, token, estimated_rows, rows_exact, estimated_bytes, null, null,
                null, partition_bounds, column_statistics, location_ids, start_position,
                end_position);
    }

    /** A split naming the given work, with no estimates.
     *
     * @param payload the worker's own bytes naming this unit of work
     * @return a split carrying that payload
     */
    public static ScanSplit of(byte[] payload) {
        return new ScanSplit(payload, new byte[0], null, false, null, null, null,
                null, null, null, null, null, null);
    }

    /** A split naming the given work, with an exact row count and a byte estimate.
     *
     * @param payload the worker's own bytes naming this unit of work
     * @param rows exact row count for this split
     * @param bytes byte estimate, used as bin-packing weight by engines that pack
     * @return a split carrying that payload and those estimates
     */
    public static ScanSplit of(byte[] payload, long rows, long bytes) {
        return new ScanSplit(payload, new byte[0], rows, true, bytes, null, null,
                null, null, null, null, null, null);
    }

    /** This split with its exact transformed key tuple. */
    public ScanSplit withKeyValue(byte[] value) {
        return new ScanSplit(payload, token, estimated_rows, rows_exact, estimated_bytes,
                value, range_index, column_bounds, partition_bounds, column_statistics,
                location_ids, start_position, end_position);
    }

    /** This split assigned to one zero-based range. */
    public ScanSplit withRangeIndex(long value) {
        return new ScanSplit(payload, token, estimated_rows, rows_exact, estimated_bytes,
                key_value, value, column_bounds, partition_bounds, column_statistics,
                location_ids, start_position, end_position);
    }

    /** This split with its framework-stamped token attached, and its payload cleared.
     *
     * <p>The payload is cleared rather than forwarded. It is sealed INTO the token,
     * and shipping the plaintext in the field beside the ciphertext made the seal
     * decorative. No client reads it — the C++ side pulls {@code token} alone — and
     * redemption recovers the payload from inside the envelope.</p>
     *
     * @param stamped the envelope the framework built around {@link #payload()}
     * @return a copy carrying that token and an empty payload
     */
    public ScanSplit withToken(byte[] stamped) {
        return new ScanSplit(new byte[0], stamped, estimated_rows, rows_exact, estimated_bytes,
                key_value, range_index, column_bounds, partition_bounds, column_statistics,
                location_ids, start_position, end_position);
    }
}
