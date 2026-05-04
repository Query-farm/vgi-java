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
}
