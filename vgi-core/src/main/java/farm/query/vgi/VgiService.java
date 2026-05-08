// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi;

import farm.query.vgi.protocol.AggregateBindRequest;
import farm.query.vgi.protocol.AggregateBindResponse;
import farm.query.vgi.protocol.AggregateCombineRequest;
import farm.query.vgi.protocol.AggregateDestructorRequest;
import farm.query.vgi.protocol.AggregateFinalizeRequest;
import farm.query.vgi.protocol.AggregateFinalizeResponse;
import farm.query.vgi.protocol.AggregateUpdateRequest;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CardinalityRequest;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.CatalogVersionResponse;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgi.protocol.TransactionBeginResponse;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.Nullable;
import farm.query.vgirpc.schema.StreamHeader;

/**
 * The VGI RPC surface served by a worker.
 *
 * <p>Wire shape varies per method. Two flavours coexist:
 *
 * <ul>
 *   <li><b>Packed</b> — the params batch has a single {@code request: binary}
 *       column carrying a serialised {@link farm.query.vgirpc.schema.ArrowSerializableRecord}
 *       payload. Used by {@code bind}/{@code init}/{@code catalog_attach} and
 *       any method whose request would contain maps/lists/structs.
 *   <li><b>Flat</b> — the params batch's columns map 1:1 to the method's
 *       parameters by snake_case name. Used by simpler catalog reads/writes.
 * </ul>
 *
 * <p>Method and parameter / record-field names are the wire contract — they
 * MUST match the canonical Python/Go {@code snake_case}.</p>
 */
public interface VgiService {

    // -----------------------------------------------------------------------
    // Function execution (packed request)
    // -----------------------------------------------------------------------

    BindResponse bind(BindRequest request, CallContext ctx);

    @StreamHeader(GlobalInitResponse.class)
    RpcStream<? extends StreamState> init(InitRequest request, CallContext ctx);

    // -----------------------------------------------------------------------
    // Table-function statistics & cardinality (packed; default = unsupported)
    // -----------------------------------------------------------------------

    /**
     * Wire shape: {@code {request: binary}}. The blob is an IPC-encoded
     * inner batch with fields {@code {bind_call: binary, bind_opaque_data:
     * binary?}} (see {@link CardinalityRequest}). The framework can't auto-
     * destructure that nested IPC for us, so we accept the outer binary and
     * unpack manually.
     */
    default farm.query.vgi.protocol.CardinalityResponse table_function_cardinality(byte[] request) {
        return new farm.query.vgi.protocol.CardinalityResponse(null, null);
    }

    /** Same wrapper shape as {@link #table_function_cardinality}. */
    default byte[] table_function_statistics(byte[] request) {
        throw new UnsupportedOperationException("table_function_statistics");
    }

    /**
     * EXPLAIN-ANALYZE diagnostics callback. Wire shape:
     * {@code {request: binary}} where the inner IPC carries {@code
     * {bind_call: binary, bind_opaque_data: binary?, global_execution_id:
     * binary}}. Result is a 1-row batch of two parallel string lists,
     * {@code keys[]} and {@code values[]}, that DuckDB renders into the
     * scan's Extra Info.
     */
    default farm.query.vgi.protocol.DynamicToStringResponse table_function_dynamic_to_string(byte[] request) {
        return new farm.query.vgi.protocol.DynamicToStringResponse(java.util.List.of(), java.util.List.of());
    }

    // -----------------------------------------------------------------------
    // Aggregate function lifecycle
    // -----------------------------------------------------------------------

    AggregateBindResponse aggregate_bind(AggregateBindRequest request);

    farm.query.vgi.protocol.AggregateUpdateResponse aggregate_update(AggregateUpdateRequest request);

    default farm.query.vgi.protocol.AggregateCombineResponse aggregate_combine(AggregateCombineRequest request) {
        return new farm.query.vgi.protocol.AggregateCombineResponse();
    }

    AggregateFinalizeResponse aggregate_finalize(AggregateFinalizeRequest request);

    default farm.query.vgi.protocol.AggregateDestructorResponse aggregate_destructor(AggregateDestructorRequest request) {
        return new farm.query.vgi.protocol.AggregateDestructorResponse();
    }

    // -----------------------------------------------------------------------
    // Catalog: attach / detach / version / transactions
    // -----------------------------------------------------------------------

    ItemsResponse catalog_catalogs();

    /** Packed: complex request body (options/version specs). */
    CatalogAttachResult catalog_attach(CatalogAttachRequest request, CallContext ctx);

    /** Flat. */
    void catalog_detach(byte[] attach_id);

    default CatalogVersionResponse catalog_version(byte[] attach_id, @Nullable byte[] transaction_id) {
        return new CatalogVersionResponse(0L);
    }

    default TransactionBeginResponse catalog_transaction_begin(byte[] attach_id) {
        return new TransactionBeginResponse(null);
    }

    default void catalog_transaction_commit(byte[] attach_id, byte[] transaction_id) {}
    default void catalog_transaction_rollback(byte[] attach_id, byte[] transaction_id) {}

    // -----------------------------------------------------------------------
    // Catalog: schemas
    // -----------------------------------------------------------------------

    ItemsResponse catalog_schemas(byte[] attach_id, @Nullable byte[] transaction_id);

    ItemsResponse catalog_schema_get(byte[] attach_id, String name, @Nullable byte[] transaction_id);

    // -----------------------------------------------------------------------
    // Catalog: schema contents
    // -----------------------------------------------------------------------

    default ItemsResponse catalog_schema_contents_tables(
            byte[] attach_id, String name, @Nullable byte[] transaction_id) {
        return ItemsResponse.empty();
    }

    default ItemsResponse catalog_schema_contents_views(
            byte[] attach_id, String name, @Nullable byte[] transaction_id) {
        return ItemsResponse.empty();
    }

    /**
     * {@code type} is dictionary-encoded on the wire (enum-like). We accept it
     * as a plain UTF-8 string here — the framework's dictionary support reads
     * the underlying string transparently.
     */
    ItemsResponse catalog_schema_contents_functions(
            byte[] attach_id,
            String name,
            @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String type,
            @Nullable byte[] transaction_id);

    default ItemsResponse catalog_schema_contents_macros(
            byte[] attach_id,
            String name,
            @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String type,
            @Nullable byte[] transaction_id) {
        return ItemsResponse.empty();
    }

    default ItemsResponse catalog_schema_contents_indexes(
            byte[] attach_id, String name, @Nullable byte[] transaction_id) {
        return ItemsResponse.empty();
    }

    // -----------------------------------------------------------------------
    // Catalog: tables / views / macros (read; default empty)
    // -----------------------------------------------------------------------

    default ItemsResponse catalog_table_get(
            byte[] attach_id, String schema_name, String name,
            @Nullable String at_unit, @Nullable String at_value,
            @Nullable byte[] transaction_id) {
        return ItemsResponse.empty();
    }

    default TableScanFunctionGetResponse catalog_table_scan_function_get(
            byte[] attach_id, String schema_name, String name,
            @Nullable String at_unit, @Nullable String at_value,
            @Nullable byte[] transaction_id) {
        throw new UnsupportedOperationException("catalog_table_scan_function_get");
    }

    default byte[] catalog_table_column_statistics_get(
            byte[] attach_id, String schema_name, String name,
            @Nullable byte[] transaction_id) {
        return new byte[0];
    }

    default ItemsResponse catalog_view_get(
            byte[] attach_id, String schema_name, String name,
            @Nullable byte[] transaction_id) {
        return ItemsResponse.empty();
    }

    default ItemsResponse catalog_macro_get(
            byte[] attach_id, String schema_name, String name,
            @Nullable byte[] transaction_id) {
        return ItemsResponse.empty();
    }
}
