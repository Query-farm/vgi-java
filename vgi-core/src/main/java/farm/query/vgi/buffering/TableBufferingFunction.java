// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.buffering;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * A VGI table-buffering (Sink+Source) function: must see every input row before
 * producing output. Lifecycle, mirroring vgi-python {@code TableBufferingFunction}:
 *
 * <ol>
 *   <li><b>{@link #process}</b> (Sink, once per input batch) — stash the batch
 *       in {@code params.storage()} and return an opaque {@code state_id}.</li>
 *   <li><b>{@link #combine}</b> (once, end-of-input) — group/merge the
 *       {@code state_ids} into {@code finalize_state_ids}, one per output
 *       stream the Source will drain.</li>
 *   <li><b>{@link #createFinalizeProducer}</b> (Source, once per
 *       {@code finalize_state_id}) — return a {@link TableProducerState} that
 *       emits one batch per tick until it calls {@code out.finish()}.</li>
 * </ol>
 *
 * <p>The function's wire {@code function_type} is {@code "table_buffering"},
 * which selects the C++ Sink+Source operator. Ordering knobs (sink/source
 * order, batch-index) ride on {@link FunctionMetadata}.</p>
 */
public interface TableBufferingFunction extends FunctionDescriptor {

    @Override String name();
    @Override FunctionMetadata metadata();
    @Override List<ArgSpec> argumentSpecs();

    /** Declare the output schema (default: passthrough = input schema). */
    BindResponse onBind(TableInOutBindParams params);

    /** Sink: stash {@code batch} and return its {@code state_id}. */
    byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params);

    /** Combine: map the collected {@code stateIds} to {@code finalize_state_ids}. */
    List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params);

    /** Source: build the producer that drains output for one finalize_state_id. */
    TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params);

    /** When true, the C++ Source phase drains finalize_state_ids serially in
     *  {@code combine()} order ({@code ParallelSource=false}). */
    default boolean sourceOrderDependent() { return false; }

    /** When true, the Sink ingests single-threaded ({@code ParallelSink=false}). */
    default boolean sinkOrderDependent() { return false; }

    /** When true, {@code process()} receives DuckDB's globally-unique batch index. */
    default boolean requiresInputBatchIndex() { return false; }
}
