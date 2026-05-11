// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.function;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;
import java.util.Map;

/**
 * Parsed arguments to a VGI function call. Positional values are scalar Arrow
 * objects (Long, Double, String, etc.), Named are similarly typed.
 *
 * <p>{@code positionalTypes} preserves the source Arrow type per positional
 * arg (TINYINT vs BIGINT, FLOAT vs DOUBLE, …) so callers that emit a
 * dynamically-typed output schema can preserve the user's exact type.
 */
public record Arguments(List<Object> positional, Map<String, Object> named,
                          List<ArrowType> positionalTypes) {

    public Arguments(List<Object> positional, Map<String, Object> named) {
        this(positional, named, List.of());
    }

    public static Arguments empty() {
        return new Arguments(List.of(), Map.of(), List.of());
    }

    public Object positionalAt(int index) {
        if (index >= positional.size()) return null;
        return positional.get(index);
    }

    public ArrowType positionalTypeAt(int index) {
        if (positionalTypes == null || index >= positionalTypes.size()) return null;
        return positionalTypes.get(index);
    }
}
