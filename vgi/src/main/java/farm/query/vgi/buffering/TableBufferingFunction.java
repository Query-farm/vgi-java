// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.buffering;

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

    /**
     * Declares the output schema (default: passthrough = input schema).
     *
     * @param params the bind-time parameters (input schema, arguments, pushdown).
     * @return the bind response carrying the declared output schema.
     */
    BindResponse onBind(TableInOutBindParams params);

    /**
     * Sink phase: stashes {@code batch} and returns its {@code state_id}.
     *
     * @param batch one input batch to buffer.
     * @param params the process-phase context (storage, execution id, batch index).
     * @return the opaque {@code state_id} naming the stashed batch's state.
     */
    byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params);

    /**
     * Combine phase: groups/merges the collected {@code stateIds} into the
     * {@code finalize_state_ids}, one per output stream the Source will drain.
     *
     * @param stateIds the {@code state_id}s returned by every {@link #process} call.
     * @param params the combine-phase context (storage, execution id, call context).
     * @return the {@code finalize_state_id}s, one per output stream.
     */
    List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params);

    /**
     * Source phase: builds the producer that drains output for one
     * {@code finalize_state_id}.
     *
     * @param params the finalize-phase context (storage, finalize state id, init params).
     * @return a producer that emits one batch per tick until it finishes.
     */
    TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params);

    /**
     * Whether the Source phase must drain {@code finalize_state_id}s serially in
     * {@code combine()} order ({@code ParallelSource=false}).
     *
     * @return {@code true} to force ordered, single-stream draining.
     */
    default boolean sourceOrderDependent() { return false; }

    /**
     * Whether the Sink must ingest single-threaded ({@code ParallelSink=false}).
     *
     * @return {@code true} to force ordered, single-threaded ingest.
     */
    default boolean sinkOrderDependent() { return false; }

    /**
     * Whether {@link #process} should receive DuckDB's globally-unique batch index.
     *
     * @return {@code true} to populate {@link TableBufferingProcessParams#batchIndex()}.
     */
    default boolean requiresInputBatchIndex() { return false; }
}
