// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.function;

/** How null inputs propagate. Mirrors vgi-go {@code NullHandling}. */
public enum NullHandling {
    /** Skip null inputs / propagate nulls automatically. */
    DEFAULT,
    /** Pass nulls through; function decides. */
    SPECIAL
}
