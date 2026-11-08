// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;

/**
 * Parameters passed to {@link TableFunction#createProducer}.
 *
 * <p>{@code executionId} is the per-execution identifier DuckDB threads
 * through every callback (init, statistics, dynamic_to_string). Producers
 * that publish per-execution diagnostics keep state keyed off this byte[]
 * so the {@link TableFunction#dynamicToString} hook can match the snapshot
 * back to its scan.
 *
 * @param functionName the invoked table function's name
 * @param arguments the positional and named call arguments
 * @param outputSchema the schema to emit, already narrowed to the projected columns
 * @param bindOutputSchema the authoritative, unprojected bind output schema used to
 *        validate v2 filter column and nested-field indexes and names
 * @param settings session settings in effect for this call
 * @param allocator the allocator producers must use for emitted batches
 * @param pushdownFilters the encoded pushdown-filter bytes, or empty when none
 * @param projectionIds the column indices DuckDB requested, in output order
 * @param joinKeys the encoded join-filter key bytes
 * @param tablesamplePercentage the {@code TABLESAMPLE} percentage, or {@code null}
 * @param tablesampleSeed the {@code TABLESAMPLE} seed, or {@code null}
 * @param orderByColumnName the pushed-down ORDER BY column, or {@code null}
 * @param orderByDirection the pushed-down ORDER BY direction, or {@code null}
 * @param orderByNullOrder the pushed-down ORDER BY null ordering, or {@code null}
 * @param orderByLimit the pushed-down ORDER BY limit, or {@code null}
 * @param executionId the per-execution identifier threaded through every callback
 * @param secrets the resolved secret bytes, or {@code null} when none were sent
 * @param attachId the catalog attach's opaque identifier bytes
 * @param bindOpaqueData the {@code BindResponse.opaque_data} the fixture's
 *     {@code onBind} returned — ships bind-time state through to the producer;
 *     empty {@code byte[0]} when the fixture returned none
 * @param atUnit the time-travel AT clause unit threaded from the bind request
 *     ({@code init_call.bind_call.at_unit}), or {@code null} when the scan has no AT clause
 * @param atValue the time-travel AT clause value, or {@code null} when absent
 * @param storage the per-execution shared-state facade (scoped to
 *     {@code executionId}, shard-pinned to the attach) — mirrors vgi-python's
 *     {@code params.storage}
 * @param copyFrom the {@code COPY ... FROM} context, set only when this init drives a
 * @param splitPayloads the VERIFIED payloads of the splits this init claimed —
 *        the worker's own bytes, with the token envelope already opened and
 *        stripped, so an unverified token never reaches a function. {@code null}
 *        when this is not a split init, which is different from an empty list:
 *        null means the client did not plan, empty is a claim of no work.
 *     COPY-FROM scan (see {@link farm.query.vgi.table.CopyFromFunction}); {@code null}
 *     for every ordinary table-function init
 * @param transactionId the plaintext id of the DuckDB transaction this scan runs in (the
 *     same bytes {@link farm.query.vgi.TransactionHooks#begin} returned and the commit /
 *     rollback hooks receive), or {@code null} outside a transaction-supporting attach. A
 *     producer that needs it while producing rows keeps it in a field of its state.
 * @param attachOptions the attach's ATTACH option values, secret ones included; never
 *     {@code null}
 */
public record TableInitParams(
        String functionName,
        Arguments arguments,
        Schema outputSchema,
        Schema bindOutputSchema,
        Map<String, Object> settings,
        BufferAllocator allocator,
        byte[] pushdownFilters,
        List<Integer> projectionIds,
        List<byte[]> joinKeys,
        PushdownFiltersDecoder.Capabilities filterCapabilities,
        Double tablesamplePercentage,
        Long tablesampleSeed,
        String orderByColumnName,
        String orderByDirection,
        String orderByNullOrder,
        Long orderByLimit,
        byte[] executionId,
        byte[] secrets,
        byte[] attachId,
        byte[] bindOpaqueData,
        String atUnit,
        String atValue,
        farm.query.vgi.storage.BoundStorage storage,
        farm.query.vgi.protocol.CopyFromContext copyFrom,
        List<byte[]> splitPayloads,
        byte[] transactionId,
        farm.query.vgi.AttachOptions attachOptions) {

    /** Normalizes a missing option set to the non-secret options the attach id carries. */
    public TableInitParams {
        if (attachOptions == null) attachOptions = farm.query.vgi.AttachOptions.fromAttachId(attachId);
    }

    /**
     * Compatibility constructor of releases before transaction ids and attach options.
     *
     * @param functionName the invoked table function's name
     * @param arguments the positional and named call arguments
     * @param outputSchema the schema to emit
     * @param bindOutputSchema the unprojected bind output schema
     * @param settings session settings in effect for this call
     * @param allocator the allocator producers must use
     * @param pushdownFilters the encoded pushdown-filter bytes
     * @param projectionIds the requested column indices
     * @param joinKeys the encoded join-filter key bytes
     * @param filterCapabilities the filter capabilities
     * @param tablesamplePercentage the TABLESAMPLE percentage, or {@code null}
     * @param tablesampleSeed the TABLESAMPLE seed, or {@code null}
     * @param orderByColumnName the ORDER BY column, or {@code null}
     * @param orderByDirection the ORDER BY direction, or {@code null}
     * @param orderByNullOrder the ORDER BY null ordering, or {@code null}
     * @param orderByLimit the ORDER BY limit, or {@code null}
     * @param executionId the per-execution identifier
     * @param secrets the resolved secret bytes, or {@code null}
     * @param attachId the attach's plain identifier bytes
     * @param bindOpaqueData the bind's opaque data
     * @param atUnit the AT clause unit, or {@code null}
     * @param atValue the AT clause value, or {@code null}
     * @param storage the per-execution storage facade
     * @param copyFrom the COPY-FROM context, or {@code null}
     * @param splitPayloads the verified split payloads, or {@code null}
     */
    public TableInitParams(String functionName, Arguments arguments, Schema outputSchema,
            Schema bindOutputSchema, Map<String, Object> settings, BufferAllocator allocator,
            byte[] pushdownFilters, List<Integer> projectionIds, List<byte[]> joinKeys,
            PushdownFiltersDecoder.Capabilities filterCapabilities, Double tablesamplePercentage,
            Long tablesampleSeed, String orderByColumnName, String orderByDirection,
            String orderByNullOrder, Long orderByLimit, byte[] executionId, byte[] secrets,
            byte[] attachId, byte[] bindOpaqueData, String atUnit, String atValue,
            farm.query.vgi.storage.BoundStorage storage, farm.query.vgi.protocol.CopyFromContext copyFrom,
            List<byte[]> splitPayloads) {
        this(functionName, arguments, outputSchema, bindOutputSchema, settings, allocator,
                pushdownFilters, projectionIds, joinKeys, filterCapabilities, tablesamplePercentage,
                tablesampleSeed, orderByColumnName, orderByDirection, orderByNullOrder, orderByLimit,
                executionId, secrets, attachId, bindOpaqueData, atUnit, atValue, storage, copyFrom,
                splitPayloads, null, null);
    }

    /**
     * Wrap the raw {@link #pushdownFilters} / {@link #joinKeys} bytes into a
     * {@link FilterApplier} ready to call {@code apply(root)} on each emitted
     * batch. Cheap to construct — the actual decode happens lazily on first
     * {@code apply}. Producers should cache the returned applier on their
     * state (see {@link TableProducerState}) so the decode runs once.
     *
     * @return a filter applier over the pushdown filters and join keys
     */
    public FilterApplier filters() {
        return FilterApplier.from(pushdownFilters, joinKeys, bindOutputSchema, filterCapabilities);
    }

    /** Decode the current v2 snapshot against the authoritative bind output schema. */
    public PushdownFilters decodeFilters() {
        if (pushdownFilters == null || pushdownFilters.length == 0) return PushdownFilters.empty();
        return PushdownFiltersDecoder.decode(
                pushdownFilters, bindOutputSchema, joinKeys, filterCapabilities);
    }

    /**
     * Alias for {@link #outputSchema()} — kept for symmetry with
     * {@link #projectionIds()} and to document the projection contract:
     * the framework ({@code VgiServiceImpl.initTable}) already narrowed
     * {@code outputSchema} down to the columns DuckDB requested
     * <em>before</em> handing the params to the fixture, so the schema
     * delivered here matches the batches the fixture must emit. Use
     * {@code outputSchema()} directly in new code; this method exists for
     * pre-fix callers who expected to do the projection themselves.
     *
     * @return the already-projected {@link #outputSchema()}
     * @deprecated Use {@link #outputSchema()}. The previous implementation
     *     re-projected over the already-projected wire schema and produced
     *     an empty result for {@code count(*)}-style queries.
     */
    @Deprecated
    public Schema projectedOutputSchema() {
        return outputSchema;
    }
}
