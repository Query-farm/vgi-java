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
