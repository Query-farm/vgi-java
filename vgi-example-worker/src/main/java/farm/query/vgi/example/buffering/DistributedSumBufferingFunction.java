// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.buffering;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;

/**
 * {@code sum_all_columns_simple_distributed(data TABLE)} — same column-wise
 * accumulation as {@link SumAllColumnsBufferingFunction} under a different
 * registered name; the integration suite exercises the "distributed" variant
 * with multi-batch inputs. Like the parent it is a {@code TABLE_BUFFERING}
 * Sink+Source function, so the accumulated partial sums live in the worker's
 * storage keyed by {@code execution_id} and survive the stateless HTTP
 * process → combine → finalize round-trip (the old streaming table-in-out
 * shape held the running sums in an in-process exchange-state map that HTTP
 * never updated, so finalize read zeros). Mirrors vgi-python's
 * {@code SumAllColumnsSimpleDistributed}.
 */
public final class DistributedSumBufferingFunction extends SumAllColumnsBufferingFunction {

    // Only the {@code data} table arg (no {@code logging}) — matches the
    // canonical SingleTableArguments the distributed variant declares.
    private static final FunctionSpec SPEC = FunctionSpec.builder("sum_all_columns_simple_distributed")
            .metadata(FunctionMetadata.describe("Distributed sum using simple callback API")
                    .withCategories("aggregation", "numeric", "distributed"))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
}
