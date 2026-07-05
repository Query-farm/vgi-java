// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import com.google.gson.Gson;
import com.google.gson.JsonPrimitive;
import farm.query.vgi.function.ArgSpec;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encode/decode an {@link ArgSpec} list as an Arrow {@link Schema}.
 *
 * <p>Mirrors {@code vgi-python/vgi/argument_spec.py argument_specs_to_schema}:
 * <ul>
 *   <li>Each ArgSpec → one Schema {@link Field}.</li>
 *   <li>Field name = ArgSpec name.</li>
 *   <li>Field type = ArgSpec arrowType.</li>
 *   <li>Field metadata carries {@code vgi_arg=named}, {@code vgi_const=true},
 *       {@code vgi_varargs=true} as needed.</li>
 * </ul>
 *
 * <p>It also emits the per-argument constraint metadata used for agent
 * discovery, byte-for-byte matching vgi-python's {@code argument_spec.py} and
 * read by the C++ {@code vgi_function_arguments()} diagnostic. All four keys are
 * presence-only (omitted when absent; never emitted with an empty value):
 * <ul>
 *   <li>{@code vgi_default} — JSON encoding of the argument's default value
 *       (from {@link ArgSpec#hasDefault()} / {@link ArgSpec#defaultValue()});
 *       only when a non-empty default literal is present.</li>
 *   <li>{@code vgi_choices} — JSON array of the closed set of allowed values.</li>
 *   <li>{@code vgi_range} — interval notation built from {@code ge}/{@code le}/
 *       {@code gt}/{@code lt} (e.g. {@code "[0, 10]"}, {@code "(0, +inf)"}).</li>
 *   <li>{@code vgi_pattern} — the raw regex the value must match.</li>
 * </ul>
 */
public final class ArgumentSpecSerializer {

    /** Field-metadata key: JSON encoding of an argument's default value. */
    static final String VGI_DEFAULT_KEY = "vgi_default";
    /** Field-metadata key: JSON array of an argument's allowed values. */
    static final String VGI_CHOICES_KEY = "vgi_choices";
    /** Field-metadata key: interval-notation string of an argument's numeric bounds. */
    static final String VGI_RANGE_KEY = "vgi_range";
    /** Field-metadata key: raw regex an argument's value must match. */
    static final String VGI_PATTERN_KEY = "vgi_pattern";

    private static final Gson GSON = new Gson();

    private ArgumentSpecSerializer() {}

    /**
     * Encode argument specs as a {@link Schema} — one field per spec, positional
     * first (by position), then named, with VGI metadata flags applied.
     *
     * @param specs the argument specs to encode
     * @return the schema describing the function's arguments
     */
    public static Schema toSchema(List<ArgSpec> specs) {
        List<Field> fields = new ArrayList<>(specs.size());
        // Sort: positional first by position, named after.
        List<ArgSpec> sorted = new ArrayList<>(specs);
        sorted.sort((a, b) -> {
            int ka = a.position() >= 0 ? a.position() : Integer.MAX_VALUE;
            int kb = b.position() >= 0 ? b.position() : Integer.MAX_VALUE;
            return Integer.compare(ka, kb);
        });
        for (ArgSpec spec : sorted) {
            Map<String, String> meta = new HashMap<>();
            if (spec.position() < 0) meta.put("vgi_arg", "named");
            if (spec.isConst()) meta.put("vgi_const", "true");
            if (spec.varargs()) meta.put("vgi_varargs", "true");
            if (spec.anyType()) meta.put("vgi_type", "any");
            if (spec.tableInput()) meta.put("vgi_type", "table");
            // Per-argument description (UTF-8; presence-only — omit when empty).
            if (spec.doc() != null && !spec.doc().isEmpty()) meta.put("vgi_doc", spec.doc());
            // Per-argument constraint metadata (all presence-only; see class doc).
            applyConstraintMetadata(spec, meta);
            FieldType ft = new FieldType(true, spec.arrowType(), null,
                    meta.isEmpty() ? null : meta);
            fields.add(new Field(spec.name(), ft, spec.children()));
        }
        return new Schema(fields);
    }

    /**
     * Populate {@code meta} with the presence-only constraint keys for {@code spec}:
     * {@code vgi_default} (from the default concept), {@code vgi_choices},
     * {@code vgi_range}, and {@code vgi_pattern}.
     */
    private static void applyConstraintMetadata(ArgSpec spec, Map<String, String> meta) {
        // vgi_default — the arg's default value, encoded as a JSON scalar. Java's
        // default concept is hasDefault + a non-empty literal (an empty literal is
        // the named-arg "no meaningful default" sentinel, so it is treated absent).
        if (spec.hasDefault() && spec.defaultValue() != null && !spec.defaultValue().isEmpty()) {
            meta.put(VGI_DEFAULT_KEY, encodeDefault(spec.arrowType(), spec.defaultValue()));
        }

        ArgSpec.Constraints c = spec.constraints();
        if (c == null) {
            return;
        }
        if (c.choices() != null) {
            meta.put(VGI_CHOICES_KEY, encodeChoices(c.choices()));
        }
        String range = formatRange(c.ge(), c.le(), c.gt(), c.lt());
        if (range != null) {
            meta.put(VGI_RANGE_KEY, range);
        }
        if (c.pattern() != null && !c.pattern().isEmpty()) {
            meta.put(VGI_PATTERN_KEY, c.pattern());
        }
    }

    /**
     * Encode an argument's default literal as a JSON scalar, choosing quoting by
     * the declared Arrow type (numeric / boolean unquoted, everything else a JSON
     * string). Falls back to a JSON string when the literal does not parse as the
     * declared numeric/boolean type.
     */
    private static String encodeDefault(ArrowType type, String literal) {
        try {
            if (type instanceof ArrowType.Bool) {
                return new JsonPrimitive(Boolean.parseBoolean(literal.trim())).toString();
            }
            if (type instanceof ArrowType.Int) {
                return new JsonPrimitive(Long.valueOf(literal.trim())).toString();
            }
            if (type instanceof ArrowType.FloatingPoint || type instanceof ArrowType.Decimal) {
                return new JsonPrimitive(Double.valueOf(literal.trim())).toString();
            }
        } catch (RuntimeException ignored) {
            // Not parseable as the declared numeric/boolean type — emit as a JSON string.
        }
        return new JsonPrimitive(literal).toString();
    }

    /**
     * Encode the closed set of allowed values as a JSON array. On any
     * serialization failure, falls back to a JSON array of each value's
     * {@link String#valueOf(Object)} form rather than dropping the registration.
     */
    private static String encodeChoices(List<Object> choices) {
        try {
            return GSON.toJson(choices);
        } catch (RuntimeException e) {
            List<String> reprs = new ArrayList<>(choices.size());
            for (Object value : choices) {
                reprs.add(String.valueOf(value));
            }
            return GSON.toJson(reprs);
        }
    }

    /**
     * Build interval notation from an argument's numeric bounds. Inclusive bounds
     * ({@code ge}/{@code le}) render as square brackets, exclusive bounds
     * ({@code gt}/{@code lt}) as parentheses, and an open side as
     * {@code -inf}/{@code +inf}. Returns {@code null} when every bound is absent.
     */
    private static String formatRange(Number ge, Number le, Number gt, Number lt) {
        if (ge == null && le == null && gt == null && lt == null) {
            return null;
        }
        String low;
        if (gt != null) {
            low = "(" + formatBound(gt);
        } else if (ge != null) {
            low = "[" + formatBound(ge);
        } else {
            low = "(-inf";
        }
        String high;
        if (lt != null) {
            high = formatBound(lt) + ")";
        } else if (le != null) {
            high = formatBound(le) + "]";
        } else {
            high = "+inf)";
        }
        return low + ", " + high;
    }

    /**
     * Render a numeric bound for interval notation: an integral value prints
     * without a trailing {@code .0} (so a boxed {@code 0.0} becomes {@code "0"}),
     * a fractional value prints as-is.
     */
    private static String formatBound(Number n) {
        if (n instanceof Double || n instanceof Float) {
            double d = n.doubleValue();
            if (!Double.isNaN(d) && !Double.isInfinite(d) && d == Math.rint(d)) {
                return Long.toString((long) d);
            }
        }
        return n.toString();
    }

    /**
     * Encode argument specs to IPC schema bytes.
     *
     * @param specs the argument specs to encode
     * @return the IPC-encoded schema bytes
     */
    public static byte[] toIpcBytes(List<ArgSpec> specs) {
        return SchemaUtil.serializeSchema(toSchema(specs));
    }
}
