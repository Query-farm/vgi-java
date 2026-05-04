// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.function;

/** Function output determinism. Mirrors vgi-go {@code FunctionStability}. */
public enum Stability {
    CONSISTENT,
    CONSISTENT_WITHIN_QUERY,
    VOLATILE
}
