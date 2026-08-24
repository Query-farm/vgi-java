// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.Nullable;
import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Wire DTO for the {@code table_function_plan} request: the scan-planning phase
 * that precedes per-split {@code init()}.
 *
 * <p>{@code plan()} runs once with the pushdown filters and returns named
 * splits; each split is then redeemed by {@code init()} from any process or
 * host. This is what makes the scan sound in a distributed engine, where a
 * retried task must be able to re-request exactly the work it was given.</p>
 *
 * <p>Field order and nullability mirror {@code vgi/protocol.py}'s
 * {@code TableFunctionPlanRequest} exactly — this is the schema the C++
 * extension (and any other client) sends as the {@code request} outer binary
 * to {@code VgiService#table_function_plan}, one row, no wrapping struct
 * beyond what {@link farm.query.vgirpc.marshal.RecordCodec#serializeToBytes}
 * already produces for any {@link ArrowSerializableRecord}.</p>
 *
 * @param bind_call the originating serialised {@link BindRequest}
 * @param bind_opaque_data opaque per-bind state the worker returned from bind()
 * @param projection_ids column indices the client wants projected, or {@code null}
 * @param pushdown_filters serialised static filter predicates, or {@code null}.
 *        A plan is built from STATIC filters only — join-key values are not
 *        known when this fires, so they cannot prune the split SET; they
 *        arrive later, per tick, and prune WITHIN each split
 * @param join_keys serialised join-key batches pushed down for join filtering,
 *        or {@code null}
 * @param row_limit a plain fetch limit, or {@code null}. NOT
 *        {@code order_by_limit} (the Top-N hint's own field). DuckDB cannot
 *        supply this — its init input carries no limit field — so it is
 *        always {@code null} from DuckDB; DataFusion supplies it from
 *        {@code TableProvider::scan(limit)}
 * @param target_split_bytes requested split size, or {@code null}. The
 *        primary sizing lever: a worker should emit splits of comparable
 *        cost, because the client cannot see per-split cost and will claim
 *        them as interchangeable units
 * @param min_splits parallelism floor, or {@code null} when the client has no
 *        opinion — a small but expensive table still needs enough splits to
 *        occupy the client's readers
 * @param max_splits_per_response pagination cap (Trino
 *        {@code ConnectorSplitSource.getNextBatch(maxSize)}), NOT a sizing
 *        hint, or {@code null} for no cap
 * @param cursor resume point in the ENUMERATION of splits, or {@code null} on
 *        the first call. Distinct from {@code start_position}, which names a
 *        place in the DATA
 * @param refined_filters conjunctive narrowing on a continuation, or
 *        {@code null}. Narrows future splits only; splits already emitted
 *        under a looser filter stay valid
 * @param filters_complete {@code false} means more refinement may arrive, so
 *        the worker may hold back splits; {@code true} says stop waiting
 * @param start_position exclusive lower bound in the data, or {@code null}
 *        meaning from the start
 * @param end_position inclusive upper bound in the data, or {@code null}
 *        meaning "as of now" — the worker reports the frontier it resolved in
 *        {@code PlanResponse.end_position}
 * @param order_by_column_name column of the order-pushdown hint, or {@code null}
 * @param order_by_direction direction of the order-pushdown hint, or {@code null}
 * @param order_by_null_order null ordering of the order-pushdown hint, or {@code null}
 * @param order_by_limit row limit of the Top-N order-pushdown hint, or {@code null}
 * @param tablesample_percentage TABLESAMPLE percentage hint, or {@code null}
 * @param tablesample_seed TABLESAMPLE seed hint, or {@code null}
 */
public record TableFunctionPlanRequest(
        byte[] bind_call,
        @Nullable byte[] bind_opaque_data,
        @Nullable List<Integer> projection_ids,
        @Nullable @ArrowField(ArrowFieldType.LARGE_BINARY) byte[] pushdown_filters,
        @Nullable @ArrowField(ArrowFieldType.LARGE_BINARY) List<byte[]> join_keys,
        @Nullable Long row_limit,
        @Nullable Long target_split_bytes,
        @Nullable Long min_splits,
        @Nullable Long max_splits_per_response,
        @Nullable byte[] cursor,
        @Nullable @ArrowField(ArrowFieldType.LARGE_BINARY) byte[] refined_filters,
        boolean filters_complete,
        @Nullable byte[] start_position,
        @Nullable byte[] end_position,
        @Nullable String order_by_column_name,
        @Nullable String order_by_direction,
        @Nullable String order_by_null_order,
        @Nullable Long order_by_limit,
        @Nullable Double tablesample_percentage,
        @Nullable Long tablesample_seed) implements ArrowSerializableRecord {

    /**
     * A first plan call for the given binding: no pushdown beyond what
     * {@code bind_call} already carries, no cursor, no sizing opinion.
     *
     * @param bindCall the serialised {@link BindRequest} that opened this scan
     * @param bindOpaqueData the {@code BindResponse.opaque_data} handle, or
     *        {@code null} if the caller has none
     * @return a minimal plan request for that binding
     */
    public static TableFunctionPlanRequest of(byte[] bindCall, byte[] bindOpaqueData) {
        return new TableFunctionPlanRequest(bindCall, bindOpaqueData,
                null, null, null, null, null, null, null, null, null, true,
                null, null, null, null, null, null, null, null);
    }

    /**
     * This request, resuming from a continuation cursor with a fresh
     * pagination cap — the shape a client's {@code getNextBatch(maxSize)}
     * loop sends on every call after the first.
     *
     * @param cursor the continuation cursor from a prior {@code PlanResponse.next_cursors}
     * @param maxSplitsPerResponse this call's pagination cap
     * @return a copy carrying that cursor and cap
     */
    public TableFunctionPlanRequest withCursor(byte[] cursor, long maxSplitsPerResponse) {
        return new TableFunctionPlanRequest(bind_call, bind_opaque_data, projection_ids,
                pushdown_filters, join_keys, row_limit, target_split_bytes, min_splits,
                maxSplitsPerResponse, cursor, refined_filters, filters_complete,
                start_position, end_position, order_by_column_name, order_by_direction,
                order_by_null_order, order_by_limit, tablesample_percentage, tablesample_seed);
    }

    /**
     * This request with a pagination cap set — the shape a client's first
     * {@code getNextBatch(maxSize)} call sends.
     *
     * @param maxSplitsPerResponse the pagination cap
     * @return a copy carrying that cap
     */
    public TableFunctionPlanRequest withMaxSplitsPerResponse(long maxSplitsPerResponse) {
        return new TableFunctionPlanRequest(bind_call, bind_opaque_data, projection_ids,
                pushdown_filters, join_keys, row_limit, target_split_bytes, min_splits,
                maxSplitsPerResponse, cursor, refined_filters, filters_complete,
                start_position, end_position, order_by_column_name, order_by_direction,
                order_by_null_order, order_by_limit, tablesample_percentage, tablesample_seed);
    }
}
