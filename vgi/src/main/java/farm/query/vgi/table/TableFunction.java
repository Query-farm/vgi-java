// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.protocol.BindResponse;


/**
 * A VGI table function: generates a stream of {@link org.apache.arrow.vector.VectorSchemaRoot}
 * batches with no input columns. Mirrors {@code vgi.TableFunction} in vgi-go.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #onBind} — validate args, return output schema.</li>
 *   <li>{@link #createProducer} — instantiate per-execution producer state.</li>
 *   <li>The framework drives the producer state's {@code produce()} repeatedly
 *       until it signals {@code out.finish()} or emits no batch.</li>
 * </ol>
 */
public interface TableFunction extends FunctionDescriptor {

    /**
     * Validates the call arguments and returns the output schema.
     *
     * @param params the bind-time parameters
     * @return the bind response carrying the output schema (and any opaque data)
     */
    BindResponse onBind(TableBindParams params);

    /**
     * Builds a fresh per-call producer. The framework owns the returned object.
     *
     * @param params the per-execution init parameters
     * @return a new producer state driven once per tick
     */
    TableProducerState createProducer(TableInitParams params);

    /**
     * Cardinality estimate for the result of this function call. The returned
     * value is forwarded to DuckDB's optimiser via the
     * {@code table_function_cardinality} RPC. {@code -1} means "unknown".
     *
     * @param params the bind-time parameters
     * @return the estimated row count, or {@code -1} when unknown
     */
    default long cardinality(TableBindParams params) { return -1L; }

    /**
     * Divides this scan into named, independently redeemable splits.
     *
     * <p>Returning an empty list (the default) means this function is not
     * split-capable: the whole scan is one unit of work, and the client falls
     * back to primary/secondary init. Override together with
     * {@code FunctionMetadata.supportsSplits}.</p>
     *
     * <p>A split <em>names</em> work rather than describing it. "These three
     * files at version 47" survives a retry; "rows 0-999 of whatever this
     * returns now" does not — and a distributed engine WILL retry, so the
     * difference is correctness, not tidiness. The same split may also be
     * redeemed more than once (recursive CTEs, task retry) and may be abandoned
     * mid-stream (LIMIT, an empty join build side); neither is an error.</p>
     *
     * <p>Set only {@code payload} on each split. The framework stamps the
     * consistency anchor, the bind fingerprint and — where a signing key exists
     * — the seal, so an author cannot forget the anchor or mis-bind the
     * fingerprint, and never writes crypto.</p>
     *
     * <p>Returns a {@link PlanResult} rather than a bare split list so a worker
     * can also pin the plan's {@code catalogVersion} and continue enumeration
     * with a cursor. A bare list could express neither, which left two shapes —
     * a plan that has outlived its snapshot, and an enumeration too large for
     * one response — inexpressible in this SDK alone.</p>
     *
     * @param params the bind-time parameters for this scan
     * @param request the plan call, carrying pushdown and the enumeration cursor
     * @return the splits and plan-level facts; empty means "not split-capable"
     */
    default PlanResult plan(TableBindParams params, PlanRequest request) {
        return PlanResult.none();
    }

    /**
     * Called on a split init with the VERIFIED payloads this connection claimed,
     * also available on {@code TableInitParams.splitPayloads()}.
     *
     * <p>Any state carried from planning to reading must live in cross-process
     * storage keyed by {@code execution_id}: the process that plans is, in the
     * general case, not the process that reads — and under a distributed engine
     * it is not even the same host.</p>
     *
     * @param payloads the worker's own bytes, envelope already stripped
     * @param params the bind-time parameters for this scan
     */
    default void onSplit(java.util.List<byte[]> payloads, TableBindParams params) {
        // Declaring the capability is enough for a function that reads its
        // ranges from TableInitParams.splitPayloads().
    }

    /**
     * Hint to DuckDB how many parallel workers may scan this function. The
     * value is sent in {@code GlobalInitResponse.max_workers}. Default 1
     * (single-worker). Functions that share state across workers via a
     * thread-safe queue should override.
     *
     * @return the maximum number of parallel scan workers
     */
    default long maxWorkers() { return 1L; }

    /**
     * EXPLAIN-ANALYZE-time diagnostics. DuckDB calls
     * {@code table_function_dynamic_to_string} once per parallel scan thread
     * at the end of the stream, passing the per-execution {@code
     * globalExecutionId} (the same bytes the producer received via
     * {@link TableInitParams#executionId}). Implementations should look up
     * any per-execution counters / timers they accumulated during
     * {@link TableProducerState#produceTick} and return key/value pairs to
     * surface as Extra Info. Default: no extra info.
     *
     * @param globalExecutionId the per-execution identifier of the scan to report on
     * @return ordered key/value diagnostics to surface as Extra Info
     */
    default java.util.LinkedHashMap<String, String> dynamicToString(byte[] globalExecutionId) {
        return new java.util.LinkedHashMap<>();
    }

    /**
     * Per-output-column statistics surfaced via the
     * {@code table_function_statistics} RPC. Used by DuckDB's optimiser for
     * filter elimination. Return {@code null} or an empty list when stats are
     * unknown.
     *
     * @param params the bind-time parameters
     * @return per-output-column statistics, or {@code null}/empty when unknown
     */
    default java.util.List<farm.query.vgi.catalog.ColumnStatistics> statistics(TableBindParams params) {
        return null;
    }
}
