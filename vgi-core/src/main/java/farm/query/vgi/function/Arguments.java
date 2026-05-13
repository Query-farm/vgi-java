// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.function;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Map;

/**
 * Parsed arguments to a VGI function call. Positional values are scalar Arrow
 * objects (Long, Double, String, etc.), Named are similarly typed.
 *
 * <p>{@code positionalTypes} preserves the source Arrow type per positional
 * arg (TINYINT vs BIGINT, FLOAT vs DOUBLE, …) so callers that emit a
 * dynamically-typed output schema can preserve the user's exact type.
 * {@code positionalFields} additionally carries the source {@link Field}
 * including {@code ARROW:extension:*} metadata, which DuckDB uses to round-
 * trip types like HUGEINT (sent as {@code FixedSizeBinary(16)} with
 * extension {@code arrow.opaque} / {@code type_name=hugeint}). Callers
 * that need lossless type round-trip should rebuild output fields from
 * {@code positionalFieldAt} rather than {@code positionalTypeAt}.
 */
public record Arguments(List<Object> positional, Map<String, Object> named,
                          List<ArrowType> positionalTypes,
                          List<Field> positionalFields) {

    public Arguments(List<Object> positional, Map<String, Object> named) {
        this(positional, named, List.of(), List.of());
    }

    public Arguments(List<Object> positional, Map<String, Object> named,
                       List<ArrowType> positionalTypes) {
        this(positional, named, positionalTypes, List.of());
    }

    public static Arguments empty() {
        return new Arguments(List.of(), Map.of(), List.of(), List.of());
    }

    public Object positionalAt(int index) {
        if (index >= positional.size()) return null;
        return positional.get(index);
    }

    public ArrowType positionalTypeAt(int index) {
        if (positionalTypes == null || index >= positionalTypes.size()) return null;
        return positionalTypes.get(index);
    }

    /** Source {@link Field} for the positional argument at {@code index},
     *  including any {@code ARROW:extension:*} metadata (DuckDB lossless
     *  type tagging). Returns {@code null} when not available. */
    public Field positionalFieldAt(int index) {
        if (positionalFields == null || index >= positionalFields.size()) return null;
        return positionalFields.get(index);
    }

    /** Typed accessor for a named long argument with a default. */
    public long namedLong(String name, long defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : ((Number) v).longValue();
    }

    /** Typed accessor for a named double argument with a default. */
    public double namedDouble(String name, double defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : ((Number) v).doubleValue();
    }

    /** Typed accessor for a named boolean argument with a default. */
    public boolean namedBool(String name, boolean defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : (Boolean) v;
    }

    /** Typed accessor for a named string argument with a default. */
    public String namedString(String name, String defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : v.toString();
    }

    /** Typed accessor for a positional string argument with a default. */
    public String positionalString(int index, String defaultValue) {
        Object v = positionalAt(index);
        return v == null ? defaultValue : v.toString();
    }

    /** Typed accessor for a positional long argument with a default. */
    public long positionalLong(int index, long defaultValue) {
        Object v = positionalAt(index);
        return v == null ? defaultValue : ((Number) v).longValue();
    }

    /** Typed accessor for a positional double argument with a default. */
    public double positionalDouble(int index, double defaultValue) {
        Object v = positionalAt(index);
        return v == null ? defaultValue : ((Number) v).doubleValue();
    }

    /** Typed accessor for a positional boolean argument with a default. */
    public boolean positionalBool(int index, boolean defaultValue) {
        Object v = positionalAt(index);
        return v == null ? defaultValue : (Boolean) v;
    }
}
