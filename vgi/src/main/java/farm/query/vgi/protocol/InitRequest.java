// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.Nullable;
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
 * @param split_tokens            the framework-stamped envelopes for the splits
 *     this init redeems, or {@code null} on a non-split init. A split NAMES a
 *     unit of work, so a redemption reads the same rows however many times it
 *     runs and in whichever process — which is what lets a distributed engine
 *     retry a task. The list is length 1 from DuckDB (one greedy claim per
 *     reader) and longer from an engine that bin-packs at planning time.
 * @param row_limit               a plain fetch limit the worker may stop at, or
 *     {@code null}. Always {@code null} from DuckDB, whose init input carries no
 *     limit field; DataFusion supplies it via {@code TableProvider::scan}. Under
 *     splits the FULL limit is pushed into every split — over-production is legal
 *     and the engine re-applies above the union, whereas dividing by the split
 *     count would under-produce under skew.
 */
public record InitRequest(
        byte[] bind_call,
        byte[] output_schema,
        @Nullable byte[] bind_opaque_data,
        @Nullable List<Integer> projection_ids,
        @Nullable @ArrowField(ArrowFieldType.LARGE_BINARY) byte[] pushdown_filters,
        @Nullable @ArrowField(ArrowFieldType.LARGE_BINARY) List<byte[]> join_keys,
        @Nullable String phase,
        @Nullable byte[] execution_id,
        @Nullable byte[] init_opaque_data,
        @Nullable String order_by_column_name,
        @Nullable String order_by_direction,
        @Nullable String order_by_null_order,
        @Nullable Long order_by_limit,
        @Nullable Double tablesample_percentage,
        @Nullable Long tablesample_seed,
        @Nullable byte[] finalize_state_id,
        @Nullable byte[] substream_id,
        @Nullable @ArrowField(ArrowFieldType.LARGE_BINARY) List<byte[]> split_tokens,
        @Nullable Long row_limit) implements ArrowSerializableRecord {}
