// Copyright 2025-2026 Query.Farm LLC

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

    /** Same wrapper shape as {@link #table_function_cardinality}.
     *  Default returns empty bytes: the C++ extension interprets that as
     *  "no statistics available" and falls through to non-optimized scan. */
    default byte[] table_function_statistics(byte[] request) {
        return new byte[0];
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
    // Table buffering (Sink+Source) lifecycle. Packed requests; flat responses.
    // -----------------------------------------------------------------------

    default farm.query.vgi.protocol.TableBufferingProcessResponse table_buffering_process(
            farm.query.vgi.protocol.TableBufferingProcessRequest request, CallContext ctx) {
        throw new UnsupportedOperationException("table_buffering_process");
    }

    default farm.query.vgi.protocol.TableBufferingCombineResponse table_buffering_combine(
            farm.query.vgi.protocol.TableBufferingCombineRequest request, CallContext ctx) {
        throw new UnsupportedOperationException("table_buffering_combine");
    }

    default farm.query.vgi.protocol.TableBufferingDestructorResponse table_buffering_destructor(
            farm.query.vgi.protocol.TableBufferingDestructorRequest request) {
        return new farm.query.vgi.protocol.TableBufferingDestructorResponse();
    }

    // -----------------------------------------------------------------------
    // Catalog: attach / detach / version / transactions
    // -----------------------------------------------------------------------

    ItemsResponse catalog_catalogs();

    /** Packed: complex request body (options/version specs). */
    CatalogAttachResult catalog_attach(CatalogAttachRequest request, CallContext ctx);

    /** Flat. */
    void catalog_detach(byte[] attach_opaque_data);

    default CatalogVersionResponse catalog_version(byte[] attach_opaque_data, @Nullable byte[] transaction_opaque_data) {
        return new CatalogVersionResponse(0L);
    }

    default TransactionBeginResponse catalog_transaction_begin(byte[] attach_opaque_data, CallContext ctx) {
        return new TransactionBeginResponse(null);
    }

    default void catalog_transaction_commit(byte[] attach_opaque_data, byte[] transaction_opaque_data,
                                              CallContext ctx) {}
    default void catalog_transaction_rollback(byte[] attach_opaque_data, byte[] transaction_opaque_data,
                                                CallContext ctx) {}

    // -----------------------------------------------------------------------
    // Catalog: DDL (read-only stubs)
    // The read-only example catalog rejects every DDL call with a clear
    // "catalog is read-only" error. These method signatures pin the wire
    // contract that the C++ extension sends — adding new params here without
    // matching the C++ schema would surface as "Outgoing request batch does
    // not match the wire contract".
    // -----------------------------------------------------------------------

    default void catalog_schema_create(byte[] attach_opaque_data, String name,
                                          @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String on_conflict,
                                          @Nullable String comment,
                                          java.util.Map<String, String> tags,
                                          @Nullable byte[] transaction_opaque_data) {
        throw new UnsupportedOperationException("catalog is read-only: catalog_schema_create not supported");
    }

    default void catalog_table_column_add(byte[] attach_opaque_data, String schema_name, String name,
                                              byte[] column_definition,
                                              boolean ignore_not_found,
                                              boolean if_column_not_exists,
                                              @Nullable byte[] transaction_opaque_data) {
        throw new UnsupportedOperationException("catalog is read-only: catalog_table_column_add not supported");
    }

    default void catalog_table_column_drop(byte[] attach_opaque_data, String schema_name, String name,
                                               String column_name,
                                               boolean ignore_not_found,
                                               boolean if_column_exists,
                                               @Nullable byte[] transaction_opaque_data) {
        throw new UnsupportedOperationException("catalog is read-only: catalog_table_column_drop not supported");
    }

    // -----------------------------------------------------------------------
    // Catalog: schemas
    // -----------------------------------------------------------------------

    ItemsResponse catalog_schemas(byte[] attach_opaque_data, @Nullable byte[] transaction_opaque_data);

    ItemsResponse catalog_schema_get(byte[] attach_opaque_data, String name, @Nullable byte[] transaction_opaque_data);

    // -----------------------------------------------------------------------
    // Catalog: schema contents
    // -----------------------------------------------------------------------

    default ItemsResponse catalog_schema_contents_tables(
            byte[] attach_opaque_data, String name, @Nullable byte[] transaction_opaque_data,
            CallContext ctx) {
        return ItemsResponse.empty();
    }

    default ItemsResponse catalog_schema_contents_views(
            byte[] attach_opaque_data, String name, @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }

    /**
     * {@code type} is dictionary-encoded on the wire (enum-like). We accept it
     * as a plain UTF-8 string here — the framework's dictionary support reads
     * the underlying string transparently.
     */
    ItemsResponse catalog_schema_contents_functions(
            byte[] attach_opaque_data,
            String name,
            @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String type,
            @Nullable byte[] transaction_opaque_data,
            CallContext ctx);

    default ItemsResponse catalog_schema_contents_macros(
            byte[] attach_opaque_data,
            String name,
            @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String type,
            @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }

    default ItemsResponse catalog_schema_contents_indexes(
            byte[] attach_opaque_data, String name, @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }

    // -----------------------------------------------------------------------
    // Catalog: tables / views / macros (read; default empty)
    // -----------------------------------------------------------------------

    default ItemsResponse catalog_table_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable String at_unit, @Nullable String at_value,
            @Nullable byte[] transaction_opaque_data, CallContext ctx) {
        return ItemsResponse.empty();
    }

    default TableScanFunctionGetResponse catalog_table_scan_function_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable String at_unit, @Nullable String at_value,
            @Nullable byte[] transaction_opaque_data, CallContext ctx) {
        throw new UnsupportedOperationException("catalog_table_scan_function_get");
    }

    /**
     * Multi-branch scan plan. Returns a serialised {@code ScanBranchesResult}
     * ({@code {branches: list<binary>, required_extensions: list<utf8>}}).
     * Additive successor to {@link #catalog_table_scan_function_get}: the C++
     * extension caches a per-attach capability and falls back to the legacy
     * single-function RPC only when the worker raises method-not-implemented.
     * Because that capability is cached per-attach (not per-table), a worker
     * that implements this must return a valid (possibly one-branch) result
     * for <em>every</em> scannable table, not just multi-branch ones.
     */
    default byte[] catalog_table_scan_branches_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable String at_unit, @Nullable String at_value,
            @Nullable byte[] transaction_opaque_data, CallContext ctx) {
        throw new UnsupportedOperationException("catalog_table_scan_branches_get");
    }

    default byte[] catalog_table_column_statistics_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable byte[] transaction_opaque_data, CallContext ctx) {
        return new byte[0];
    }

    default ItemsResponse catalog_view_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }

    default ItemsResponse catalog_macro_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }
}
