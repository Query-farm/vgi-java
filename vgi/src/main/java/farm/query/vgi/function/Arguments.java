// Copyright 2026 Query Farm LLC - https://query.farm

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
 *
 * @param positional      positional argument values, as plain Java scalars.
 * @param named           kwarg-style argument values keyed by name.
 * @param positionalTypes source Arrow type per positional value (preserves the user's exact type).
 * @param positionalFields source {@link Field} per positional value, including {@code ARROW:extension:*} metadata.
 */
public record Arguments(List<Object> positional, Map<String, Object> named,
                          List<ArrowType> positionalTypes,
                          List<Field> positionalFields) {

    /** Construct with no per-positional type or field metadata. */
    public Arguments(List<Object> positional, Map<String, Object> named) {
        this(positional, named, List.of(), List.of());
    }

    /** Construct with positional types but no per-positional field metadata. */
    public Arguments(List<Object> positional, Map<String, Object> named,
                       List<ArrowType> positionalTypes) {
        this(positional, named, positionalTypes, List.of());
    }

    /**
     * Empty arguments, used for speculative catalog-discovery binds.
     *
     * @return an {@code Arguments} with no positional or named values.
     */
    public static Arguments empty() {
        return new Arguments(List.of(), Map.of(), List.of(), List.of());
    }

    /**
     * Positional value at {@code index}, or {@code null} when out of range.
     *
     * @param index zero-based positional index.
     * @return the value, or {@code null} when absent.
     */
    public Object positionalAt(int index) {
        if (index >= positional.size()) return null;
        return positional.get(index);
    }

    /**
     * Source Arrow type of the positional argument at {@code index}.
     *
     * @param index zero-based positional index.
     * @return the Arrow type, or {@code null} when not available.
     */
    public ArrowType positionalTypeAt(int index) {
        if (positionalTypes == null || index >= positionalTypes.size()) return null;
        return positionalTypes.get(index);
    }

    /** Source {@link Field} for the positional argument at {@code index},
     *  including any {@code ARROW:extension:*} metadata (DuckDB lossless
     *  type tagging). Returns {@code null} when not available.
     *
     *  @param index zero-based positional index.
     *  @return the source field, or {@code null} when not available. */
    public Field positionalFieldAt(int index) {
        if (positionalFields == null || index >= positionalFields.size()) return null;
        return positionalFields.get(index);
    }

    /**
     * Typed accessor for a named long argument with a default.
     * @param name name of the kwarg.
     * @param defaultValue value returned when the kwarg is absent or null.
     * @return the long value, or {@code defaultValue}.
     * @deprecated Prefer {@link farm.query.vgi.function.ParameterExtractor}'s
     *             {@code named(name).asLong().orElse(default)} — it adds range
     *             validation and consistent error messages.
     */
    @Deprecated
    public long namedLong(String name, long defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : ((Number) v).longValue();
    }

    /**
     * Typed accessor for a named double argument with a default.
     * @param name name of the kwarg.
     * @param defaultValue value returned when the kwarg is absent or null.
     * @return the double value, or {@code defaultValue}.
     * @deprecated Prefer {@link farm.query.vgi.function.ParameterExtractor}'s
     *             {@code named(name).asDouble().orElse(default)}.
     */
    @Deprecated
    public double namedDouble(String name, double defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : ((Number) v).doubleValue();
    }

    /**
     * Typed accessor for a named boolean argument with a default.
     * @param name name of the kwarg.
     * @param defaultValue value returned when the kwarg is absent or null.
     * @return the boolean value, or {@code defaultValue}.
     * @deprecated Prefer {@link farm.query.vgi.function.ParameterExtractor}'s
     *             {@code named(name).asBool().orElse(default)}.
     */
    @Deprecated
    public boolean namedBool(String name, boolean defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : (Boolean) v;
    }

    /**
     * Typed accessor for a named string argument with a default.
     * @param name name of the kwarg.
     * @param defaultValue value returned when the kwarg is absent or null.
     * @return the string value, or {@code defaultValue}.
     * @deprecated Prefer {@link farm.query.vgi.function.ParameterExtractor}'s
     *             {@code named(name).asString().orElse(default)}.
     */
    @Deprecated
    public String namedString(String name, String defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : v.toString();
    }

    /**
     * Typed accessor for a positional string argument with a default.
     * @param index zero-based positional index.
     * @param defaultValue value returned when the positional is absent or null.
     * @return the string value, or {@code defaultValue}.
     * @deprecated Prefer {@link farm.query.vgi.function.ParameterExtractor}'s
     *             {@code positional(i, name).asString().orElse(default)}.
     */
    @Deprecated
    public String positionalString(int index, String defaultValue) {
        Object v = positionalAt(index);
        return v == null ? defaultValue : v.toString();
    }

    /**
     * Typed accessor for a positional long argument with a default.
     * @param index zero-based positional index.
     * @param defaultValue value returned when the positional is absent or null.
     * @return the long value, or {@code defaultValue}.
     * @deprecated Prefer {@link farm.query.vgi.function.ParameterExtractor}'s
     *             {@code positional(i, name).asLong().orElse(default)}.
     */
    @Deprecated
    public long positionalLong(int index, long defaultValue) {
        Object v = positionalAt(index);
        return v == null ? defaultValue : ((Number) v).longValue();
    }

    /**
     * Typed accessor for a positional double argument with a default.
     * @param index zero-based positional index.
     * @param defaultValue value returned when the positional is absent or null.
     * @return the double value, or {@code defaultValue}.
     * @deprecated Prefer {@link farm.query.vgi.function.ParameterExtractor}'s
     *             {@code positional(i, name).asDouble().orElse(default)}.
     */
    @Deprecated
    public double positionalDouble(int index, double defaultValue) {
        Object v = positionalAt(index);
        return v == null ? defaultValue : ((Number) v).doubleValue();
    }

    /**
     * Typed accessor for a positional boolean argument with a default.
     * @param index zero-based positional index.
     * @param defaultValue value returned when the positional is absent or null.
     * @return the boolean value, or {@code defaultValue}.
     * @deprecated Prefer {@link farm.query.vgi.function.ParameterExtractor}'s
     *             {@code positional(i, name).asBool().orElse(default)}.
     */
    @Deprecated
    public boolean positionalBool(int index, boolean defaultValue) {
        Object v = positionalAt(index);
        return v == null ? defaultValue : (Boolean) v;
    }
}
