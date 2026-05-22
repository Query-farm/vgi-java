// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.buffering;

import farm.query.vgi.table.TableInitParams;

/**
 * Context passed to {@link TableBufferingFunction#createFinalizeProducer} for
 * one {@code finalize_state_id}. {@link #initParams()} carries the (already
 * projection-narrowed) output schema and the pre-built pushdown filter, so a
 * finalize producer extending {@code TableProducerState} reuses the same
 * narrow-and-filter machinery as a plain table function.
 */
public record TableBufferingFinalizeParams(
        byte[] executionId,
        byte[] finalizeStateId,
        BufferingStorage storage,
        TableInitParams initParams) {}
