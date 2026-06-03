// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.buffering;

import farm.query.vgi.table.TableInitParams;

/**
 * Context passed to {@link TableBufferingFunction#createFinalizeProducer} for
 * one {@code finalize_state_id}. {@link #initParams()} carries the (already
 * projection-narrowed) output schema and the pre-built pushdown filter, so a
 * finalize producer extending {@code TableProducerState} reuses the same
 * narrow-and-filter machinery as a plain table function.
 *
 * @param executionId the opaque {@code execution_id} identifying this buffering execution.
 * @param finalizeStateId the opaque {@code finalize_state_id} naming the output stream this producer drains.
 * @param storage the storage view bound to {@code executionId}, for reading back stashed state.
 * @param initParams the table-init parameters carrying the projection-narrowed output schema and pushdown filter.
 */
public record TableBufferingFinalizeParams(
        byte[] executionId,
        byte[] finalizeStateId,
        BufferingStorage storage,
        TableInitParams initParams) {}
