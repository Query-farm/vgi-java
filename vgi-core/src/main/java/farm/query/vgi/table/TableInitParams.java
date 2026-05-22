// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.pushdown.FilterApplier;
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
 */
public record TableInitParams(
        String functionName,
        Arguments arguments,
        Schema outputSchema,
        Map<String, Object> settings,
        BufferAllocator allocator,
        byte[] pushdownFilters,
        List<Integer> projectionIds,
        List<byte[]> joinKeys,
        Double tablesamplePercentage,
        Long tablesampleSeed,
        String orderByColumnName,
        String orderByDirection,
        String orderByNullOrder,
        Long orderByLimit,
        byte[] executionId,
        byte[] secrets,
        byte[] attachId,
        /** The {@code BindResponse.opaque_data} the fixture's {@code onBind}
         *  returned — ships bind-time state through to the producer. Empty
         *  {@code byte[0]} when the fixture returned none. */
        byte[] bindOpaqueData) {

    /**
     * Wrap the raw {@link #pushdownFilters} / {@link #joinKeys} bytes into a
     * {@link FilterApplier} ready to call {@code apply(root)} on each emitted
     * batch. Cheap to construct — the actual decode happens lazily on first
     * {@code apply}. Producers should cache the returned applier on their
     * state (see {@link TableProducerState}) so the decode runs once.
     */
    public FilterApplier filters() {
        return FilterApplier.from(pushdownFilters, joinKeys);
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
     * @deprecated Use {@link #outputSchema()}. The previous implementation
     *     re-projected over the already-projected wire schema and produced
     *     an empty result for {@code count(*)}-style queries.
     */
    @Deprecated
    public Schema projectedOutputSchema() {
        return outputSchema;
    }
}
