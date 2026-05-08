// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;

import java.util.List;

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
public interface TableFunction {

    String name();

    FunctionMetadata metadata();

    List<ArgSpec> argumentSpecs();

    BindResponse onBind(TableBindParams params);

    /** Build a fresh per-call producer. The framework owns the returned object. */
    TableProducerState createProducer(TableInitParams params);

    /**
     * Cardinality estimate for the result of this function call. The returned
     * value is forwarded to DuckDB's optimiser via the
     * {@code table_function_cardinality} RPC. {@code -1} means "unknown".
     */
    default long cardinality(TableBindParams params) { return -1L; }

    /**
     * Hint to DuckDB how many parallel workers may scan this function. The
     * value is sent in {@code GlobalInitResponse.max_workers}. Default 1
     * (single-worker). Functions that share state across workers via a
     * thread-safe queue should override.
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
     */
    default java.util.LinkedHashMap<String, String> dynamicToString(byte[] globalExecutionId) {
        return new java.util.LinkedHashMap<>();
    }
}
