// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Wire DTO for the {@code aggregate_finalize} response.
 *
 * @param result_batch serialised Arrow batch of finalized per-group result rows, one row per
 *     requested group id and conforming to the request's {@code output_schema}
 */
public record AggregateFinalizeResponse(byte[] result_batch) implements ArrowSerializableRecord {}
