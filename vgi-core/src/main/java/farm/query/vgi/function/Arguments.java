// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.function;

import java.util.List;
import java.util.Map;

/**
 * Parsed arguments to a VGI function call. Positional values are scalar Arrow
 * objects (Long, Double, String, etc.), Named are similarly typed.
 *
 * <p>For Phase 1 this is a thin wrapper; richer typed accessors arrive in
 * Phase 2 alongside {@code TypeBoundPredicate} dispatch.
 */
public record Arguments(List<Object> positional, Map<String, Object> named) {

    public static Arguments empty() {
        return new Arguments(List.of(), Map.of());
    }

    public Object positionalAt(int index) {
        if (index >= positional.size()) return null;
        return positional.get(index);
    }
}
