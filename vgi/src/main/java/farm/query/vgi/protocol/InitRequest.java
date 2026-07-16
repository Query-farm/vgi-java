// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Wire DTO for the {@code init} request that binds and configures a function
 * execution before streaming begins.
 *
 * <p>The {@code phase} field selects the execution mode (e.g. plain table,
 * table-in-out, table buffering sink, or table buffering finalize/source), and
 * the remaining fields carry the pushdown, ordering, sampling, and state
 * parameters the C++ extension negotiated at bind time.</p>
 *
 * @param bind_call               IPC-encoded bind call (function name + arguments).
 * @param output_schema           IPC-encoded requested output schema.
 * @param bind_opaque_data        worker-private bind state to carry into init.
 * @param projection_ids          column indices the client projects, for pushdown.
 * @param pushdown_filters        IPC-encoded pushed-down filter expressions.
 * @param join_keys               IPC-encoded join key batches.
 * @param phase                   execution phase selector.
 * @param execution_id            execution identifier for this binding.
 * @param init_opaque_data        worker-private init state.
 * @param order_by_column_name    ORDER BY column name, when ordering is pushed down.
 * @param order_by_direction      ORDER BY direction (e.g. ascending/descending).
 * @param order_by_null_order     ORDER BY null ordering.
 * @param order_by_limit          ORDER BY / LIMIT row cap, or {@code null}.
 * @param tablesample_percentage  TABLESAMPLE percentage, or {@code null}.
 * @param tablesample_seed        TABLESAMPLE seed, or {@code null}.
 * @param finalize_state_id       finalize-phase state identifier for buffering sources.
 * @param substream_id            stable, CLIENT-minted id for a parallel streaming
 *     table-in-out substream, identical across this substream's init / every
 *     process tick / finalize. Unlike the worker-minted {@code execution_id} it
 *     survives an HTTP load balancer dispatching each request to an arbitrary
 *     backend, so a finalize landing on a different backend can still key the
 *     substream's accumulated state (in shared storage) by it. {@code null} when
 *     the client did not supply one (serial path, non-table-in-out functions,
 *     old clients).
 */
public record InitRequest(
        byte[] bind_call,
        byte[] output_schema,
        byte[] bind_opaque_data,
        List<Integer> projection_ids,
        byte[] pushdown_filters,
        List<byte[]> join_keys,
        String phase,
        byte[] execution_id,
        byte[] init_opaque_data,
        String order_by_column_name,
        String order_by_direction,
        String order_by_null_order,
        Long order_by_limit,
        Double tablesample_percentage,
        Long tablesample_seed,
        byte[] finalize_state_id,
        byte[] substream_id) implements ArrowSerializableRecord {}
