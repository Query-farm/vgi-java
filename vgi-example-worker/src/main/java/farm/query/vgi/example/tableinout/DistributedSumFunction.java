// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tableinout;

/**
 * Same accumulation as {@link SumAllColumnsFunction} under a different
 * registered name; the test fixture exercises the "distributed" variant
 * with multi-batch inputs (>2048 rows) but the wire contract is identical.
 */
public final class DistributedSumFunction extends SumAllColumnsFunction {
    @Override public String name() { return "sum_all_columns_simple_distributed"; }
}
