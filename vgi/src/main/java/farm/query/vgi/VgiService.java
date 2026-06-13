// Copyright 2026 Query Farm LLC - https://query.farm

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

    /**
     * Bind a function invocation: validate arguments and resolve the output
     * schema before any data flows.
     *
     * @param request packed bind request (function name, argument schema, settings)
     * @param ctx     per-call context (client logging, transport hints)
     * @return the bound function's output schema plus opaque bind state
     */
    BindResponse bind(BindRequest request, CallContext ctx);

    /**
     * Begin streaming execution for a previously bound function. The stream
     * header is a {@link GlobalInitResponse}; subsequent batches carry the
     * function's output.
     *
     * @param request packed init request referencing the bind state
     * @param ctx     per-call context
     * @return a stream of output batches
     */
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
     *
     * @param request outer binary blob wrapping the inner cardinality request batch
     * @return the function's estimated cardinality; default is "unknown"
     */
    default farm.query.vgi.protocol.CardinalityResponse table_function_cardinality(byte[] request) {
        return new farm.query.vgi.protocol.CardinalityResponse(null, null);
    }

    /** Same wrapper shape as {@link #table_function_cardinality}.
     *  Default returns empty bytes: the C++ extension interprets that as
     *  "no statistics available" and falls through to non-optimized scan.
     *
     * @param request outer binary blob wrapping the inner statistics request batch
     * @return serialised per-column statistics, or empty bytes for "none"
     */
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
     *
     * @param request outer binary blob wrapping the inner diagnostics request
     * @return parallel {@code keys}/{@code values} lists rendered into Extra Info
     */
    default farm.query.vgi.protocol.DynamicToStringResponse table_function_dynamic_to_string(byte[] request) {
        return new farm.query.vgi.protocol.DynamicToStringResponse(java.util.List.of(), java.util.List.of());
    }

    // -----------------------------------------------------------------------
    // Aggregate function lifecycle
    // -----------------------------------------------------------------------

    /**
     * Bind an aggregate: validate arguments and resolve the result type.
     *
     * @param request aggregate bind request
     * @return the bound aggregate's state and result schema
     */
    AggregateBindResponse aggregate_bind(AggregateBindRequest request);

    /**
     * Fold a batch of input rows into one or more aggregate states.
     *
     * @param request input rows plus the target state handles
     * @return acknowledgement of the update
     */
    farm.query.vgi.protocol.AggregateUpdateResponse aggregate_update(AggregateUpdateRequest request);

    /**
     * Merge partial aggregate states (parallel/distributed combine step).
     *
     * @param request source and target state handles to merge
     * @return acknowledgement; default is a no-op for single-state aggregates
     */
    default farm.query.vgi.protocol.AggregateCombineResponse aggregate_combine(AggregateCombineRequest request) {
        return new farm.query.vgi.protocol.AggregateCombineResponse();
    }

    /**
     * Produce final aggregate values from accumulated states.
     *
     * @param request the state handles to finalize
     * @return the finalized result batch
     */
    AggregateFinalizeResponse aggregate_finalize(AggregateFinalizeRequest request);

    /**
     * Release resources held by aggregate states.
     *
     * @param request the state handles to destroy
     * @return acknowledgement; default is a no-op
     */
    default farm.query.vgi.protocol.AggregateDestructorResponse aggregate_destructor(AggregateDestructorRequest request) {
        return new farm.query.vgi.protocol.AggregateDestructorResponse();
    }

    // -----------------------------------------------------------------------
    // Table buffering (Sink+Source) lifecycle. Packed requests; flat responses.
    // -----------------------------------------------------------------------

    /**
     * Sink phase: buffer an input batch, returning a state handle that the
     * combine/finalize phases reference.
     *
     * @param request the input batch and execution identifiers
     * @param ctx     per-call context (client logging)
     * @return the buffered state id
     */
    default farm.query.vgi.protocol.TableBufferingProcessResponse table_buffering_process(
            farm.query.vgi.protocol.TableBufferingProcessRequest request, CallContext ctx) {
        throw new UnsupportedOperationException("table_buffering_process");
    }

    /**
     * Merge buffered sink states ahead of the source/finalize phase.
     *
     * @param request the state handles to combine
     * @param ctx     per-call context
     * @return the combined finalize-state ids
     */
    default farm.query.vgi.protocol.TableBufferingCombineResponse table_buffering_combine(
            farm.query.vgi.protocol.TableBufferingCombineRequest request, CallContext ctx) {
        throw new UnsupportedOperationException("table_buffering_combine");
    }

    /**
     * Release buffered state.
     *
     * @param request the state handles to destroy
     * @param ctx     per-call context
     * @return acknowledgement; default is a no-op
     */
    default farm.query.vgi.protocol.TableBufferingDestructorResponse table_buffering_destructor(
            farm.query.vgi.protocol.TableBufferingDestructorRequest request, CallContext ctx) {
        return new farm.query.vgi.protocol.TableBufferingDestructorResponse();
    }

    // -----------------------------------------------------------------------
    // Catalog: attach / detach / version / transactions
    // -----------------------------------------------------------------------

    /**
     * List the catalogs this worker serves (the {@code catalog_catalogs()} table).
     *
     * @return one item per catalog, including release timeline and capabilities
     */
    ItemsResponse catalog_catalogs();

    /**
     * Attach a catalog: validate ATTACH options and version, returning the
     * worker's capabilities and the opaque handle echoed on later calls.
     *
     * @param request packed attach request (options, requested version)
     * @param ctx     per-call context
     * @return the attach result (capabilities, settings, secret types, opaque handle)
     */
    CatalogAttachResult catalog_attach(CatalogAttachRequest request, CallContext ctx);

    /**
     * Detach a previously attached catalog.
     *
     * @param attach_opaque_data the opaque handle from {@link #catalog_attach}
     */
    void catalog_detach(byte[] attach_opaque_data);

    /**
     * Report the catalog's current logical version for cache invalidation.
     *
     * @param attach_opaque_data      the attach handle
     * @param transaction_opaque_data optional in-flight transaction handle
     * @return the monotonically increasing catalog version; default {@code 0}
     */
    default CatalogVersionResponse catalog_version(byte[] attach_opaque_data, @Nullable byte[] transaction_opaque_data,
                                                    CallContext ctx) {
        return new CatalogVersionResponse(0L);
    }

    /**
     * Begin a transaction.
     *
     * @param attach_opaque_data the attach handle
     * @param ctx                per-call context
     * @return the new transaction's opaque handle; default has none
     */
    default TransactionBeginResponse catalog_transaction_begin(byte[] attach_opaque_data, CallContext ctx) {
        return new TransactionBeginResponse(null);
    }

    /**
     * Commit a transaction.
     *
     * @param attach_opaque_data      the attach handle
     * @param transaction_opaque_data the transaction handle from {@link #catalog_transaction_begin}
     * @param ctx                     per-call context
     */
    default void catalog_transaction_commit(byte[] attach_opaque_data, byte[] transaction_opaque_data,
                                              CallContext ctx) {}

    /**
     * Roll back a transaction.
     *
     * @param attach_opaque_data      the attach handle
     * @param transaction_opaque_data the transaction handle from {@link #catalog_transaction_begin}
     * @param ctx                     per-call context
     */
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

    /**
     * Create a schema (DDL). Read-only catalogs reject this.
     *
     * @param attach_opaque_data      the attach handle
     * @param name                    schema name to create
     * @param on_conflict             conflict policy (dictionary-encoded on the wire)
     * @param comment                 optional schema comment
     * @param tags                    schema tag key/value pairs
     * @param transaction_opaque_data optional in-flight transaction handle
     */
    default void catalog_schema_create(byte[] attach_opaque_data, String name,
                                          @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String on_conflict,
                                          @Nullable String comment,
                                          java.util.Map<String, String> tags,
                                          @Nullable byte[] transaction_opaque_data) {
        throw new UnsupportedOperationException("catalog is read-only: catalog_schema_create not supported");
    }

    /**
     * Add a column to a table (DDL). Read-only catalogs reject this.
     *
     * @param attach_opaque_data      the attach handle
     * @param schema_name             owning schema
     * @param name                    table name
     * @param column_definition       serialised column definition
     * @param ignore_not_found        skip silently when the table is missing
     * @param if_column_not_exists    skip when the column already exists
     * @param transaction_opaque_data optional in-flight transaction handle
     */
    default void catalog_table_column_add(byte[] attach_opaque_data, String schema_name, String name,
                                              byte[] column_definition,
                                              boolean ignore_not_found,
                                              boolean if_column_not_exists,
                                              @Nullable byte[] transaction_opaque_data) {
        throw new UnsupportedOperationException("catalog is read-only: catalog_table_column_add not supported");
    }

    /**
     * Drop a column from a table (DDL). Read-only catalogs reject this.
     *
     * @param attach_opaque_data      the attach handle
     * @param schema_name             owning schema
     * @param name                    table name
     * @param column_name             column to drop
     * @param ignore_not_found        skip silently when the table is missing
     * @param if_column_exists        skip when the column does not exist
     * @param transaction_opaque_data optional in-flight transaction handle
     */
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

    /**
     * List the schemas in the attached catalog.
     *
     * @param attach_opaque_data      the attach handle
     * @param transaction_opaque_data optional in-flight transaction handle
     * @return one item per schema
     */
    ItemsResponse catalog_schemas(byte[] attach_opaque_data, @Nullable byte[] transaction_opaque_data);

    /**
     * Fetch a single schema by name.
     *
     * @param attach_opaque_data      the attach handle
     * @param name                    schema name
     * @param transaction_opaque_data optional in-flight transaction handle
     * @return the matching schema item, or empty when not found
     */
    ItemsResponse catalog_schema_get(byte[] attach_opaque_data, String name, @Nullable byte[] transaction_opaque_data);

    // -----------------------------------------------------------------------
    // Catalog: schema contents
    // -----------------------------------------------------------------------

    /**
     * List the tables in a schema.
     *
     * @param attach_opaque_data      the attach handle
     * @param name                    schema name
     * @param transaction_opaque_data optional in-flight transaction handle
     * @param ctx                     per-call context
     * @return one item per table; default is empty
     */
    default ItemsResponse catalog_schema_contents_tables(
            byte[] attach_opaque_data, String name, @Nullable byte[] transaction_opaque_data,
            CallContext ctx) {
        return ItemsResponse.empty();
    }

    /**
     * List the views in a schema.
     *
     * @param attach_opaque_data      the attach handle
     * @param name                    schema name
     * @param transaction_opaque_data optional in-flight transaction handle
     * @return one item per view; default is empty
     */
    default ItemsResponse catalog_schema_contents_views(
            byte[] attach_opaque_data, String name, @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }

    /**
     * List the functions in a schema, filtered by {@code type}.
     *
     * <p>{@code type} is dictionary-encoded on the wire (enum-like). We accept it
     * as a plain UTF-8 string here — the framework's dictionary support reads
     * the underlying string transparently.
     *
     * @param attach_opaque_data      the attach handle
     * @param name                    schema name
     * @param type                    function kind to list (dictionary-encoded on the wire)
     * @param transaction_opaque_data optional in-flight transaction handle
     * @param ctx                     per-call context
     * @return one item per matching function
     */
    ItemsResponse catalog_schema_contents_functions(
            byte[] attach_opaque_data,
            String name,
            @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String type,
            @Nullable byte[] transaction_opaque_data,
            CallContext ctx);

    /**
     * List the macros in a schema, filtered by {@code type}.
     *
     * @param attach_opaque_data      the attach handle
     * @param name                    schema name
     * @param type                    macro kind to list (dictionary-encoded on the wire)
     * @param transaction_opaque_data optional in-flight transaction handle
     * @return one item per matching macro; default is empty
     */
    default ItemsResponse catalog_schema_contents_macros(
            byte[] attach_opaque_data,
            String name,
            @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String type,
            @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }

    /**
     * List the indexes in a schema.
     *
     * @param attach_opaque_data      the attach handle
     * @param name                    schema name
     * @param transaction_opaque_data optional in-flight transaction handle
     * @return one item per index; default is empty
     */
    default ItemsResponse catalog_schema_contents_indexes(
            byte[] attach_opaque_data, String name, @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }

    // -----------------------------------------------------------------------
    // Catalog: tables / views / macros (read; default empty)
    // -----------------------------------------------------------------------

    /**
     * Fetch a single table's metadata, optionally at a point in time.
     *
     * @param attach_opaque_data      the attach handle
     * @param schema_name             owning schema
     * @param name                    table name
     * @param at_unit                 time-travel unit (e.g. {@code "version"}), or null
     * @param at_value                time-travel value, or null
     * @param transaction_opaque_data optional in-flight transaction handle
     * @param ctx                     per-call context
     * @return the table item, or empty when not found
     */
    default ItemsResponse catalog_table_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable String at_unit, @Nullable String at_value,
            @Nullable byte[] transaction_opaque_data, CallContext ctx) {
        return ItemsResponse.empty();
    }

    /**
     * Resolve the scan function that produces a table's rows (legacy
     * single-function path; see {@link #catalog_table_scan_branches_get}).
     *
     * @param attach_opaque_data      the attach handle
     * @param schema_name             owning schema
     * @param name                    table name
     * @param at_unit                 time-travel unit, or null
     * @param at_value                time-travel value, or null
     * @param transaction_opaque_data optional in-flight transaction handle
     * @param ctx                     per-call context
     * @return the bound scan function for this table
     */
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
     *
     * @param attach_opaque_data      the attach handle
     * @param schema_name             owning schema
     * @param name                    table name
     * @param at_unit                 time-travel unit, or null
     * @param at_value                time-travel value, or null
     * @param transaction_opaque_data optional in-flight transaction handle
     * @param ctx                     per-call context
     * @return serialised {@code ScanBranchesResult}
     */
    default byte[] catalog_table_scan_branches_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable String at_unit, @Nullable String at_value,
            @Nullable byte[] transaction_opaque_data, CallContext ctx) {
        throw new UnsupportedOperationException("catalog_table_scan_branches_get");
    }

    /**
     * Fetch per-column statistics for a table.
     *
     * @param attach_opaque_data      the attach handle
     * @param schema_name             owning schema
     * @param name                    table name
     * @param transaction_opaque_data optional in-flight transaction handle
     * @param ctx                     per-call context
     * @return serialised column statistics, or empty bytes for "none"
     */
    default byte[] catalog_table_column_statistics_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable byte[] transaction_opaque_data, CallContext ctx) {
        return new byte[0];
    }

    /**
     * Fetch a single view's metadata.
     *
     * @param attach_opaque_data      the attach handle
     * @param schema_name             owning schema
     * @param name                    view name
     * @param transaction_opaque_data optional in-flight transaction handle
     * @return the view item, or empty when not found
     */
    default ItemsResponse catalog_view_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }

    /**
     * Fetch a single macro's metadata.
     *
     * @param attach_opaque_data      the attach handle
     * @param schema_name             owning schema
     * @param name                    macro name
     * @param transaction_opaque_data optional in-flight transaction handle
     * @return the macro item, or empty when not found
     */
    default ItemsResponse catalog_macro_get(
            byte[] attach_opaque_data, String schema_name, String name,
            @Nullable byte[] transaction_opaque_data) {
        return ItemsResponse.empty();
    }
}
