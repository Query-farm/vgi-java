// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.tableinout;

/**
 * Same accumulation as {@link SumAllColumnsFunction} under a different
 * registered name; the test fixture exercises the "distributed" variant
 * with multi-batch inputs (>2048 rows) but the wire contract is identical.
 */
public final class DistributedSumFunction extends SumAllColumnsFunction {
    private static final farm.query.vgi.function.FunctionSpec SPEC =
            farm.query.vgi.function.FunctionSpec.builder("sum_all_columns_simple_distributed")
                    .description("Distributed sum using simple callback API")
                    .table("data")
                    .build();

    @Override public farm.query.vgi.function.FunctionSpec spec() { return SPEC; }
}
