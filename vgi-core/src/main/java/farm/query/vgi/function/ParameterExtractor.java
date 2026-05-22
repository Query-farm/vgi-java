// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.function;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Fluent, validating wrapper around {@link Arguments}.
 *
 * <p>Replaces the hand-rolled cast + null-check + range-check ceremony in
 * fixture {@code onBind}/{@code createProducer} bodies. Slot ({@link #positional}
 * or {@link #named}) → constraint ({@code asLong / asDouble / asString /
 * asBool}) → terminal ({@code required / orElse}).
 *
 * <pre>{@code
 *   ParameterExtractor p = ParameterExtractor.of(params.arguments());
 *   long count    = p.positional(0, "count").asLong().ge(1).required();
 *   long batch    = p.named("batch_size").asLong().ge(1).orElse(1000L);
 *   double inc    = p.named("increment").asDouble().orElse(1.0);
 *   String layout = p.named("layout").asString().oneOf("first","middle","last").orElse("first");
 *   boolean log   = p.named("logging").asBool().orElse(false);
 * }</pre>
 *
 * <p>Semantics:
 * <ul>
 *   <li>{@code required()} throws {@link IllegalArgumentException} when the
 *       argument is missing or wire-null.
 *   <li>{@code orElse(default)} substitutes the default when the argument is
 *       missing or wire-null (matches the pre-existing {@code Arguments.namedLong(name, default)}
 *       behaviour).
 *   <li>{@code between(min, max)} is <strong>inclusive both ends</strong>.
 *   <li>{@code asDouble()} accepts {@link Number} and {@link BigDecimal}
 *       (coerced via {@code doubleValue()}) and <strong>rejects NaN / ±Inf
 *       by default</strong>; call {@code allowNonFinite()} to opt in.
 * </ul>
 *
 * <p>Not thread-safe — wraps a snapshot of {@link Arguments} taken at the
 * bind/create-producer thread and is intended to be discarded after use.
 *
 * <p>Fixtures that need lossless type-metadata round-trip
 * ({@link Field#getMetadata()} / extension types / dict-encoded enums) must
 * continue to use {@link #positionalFieldAt(int)} directly; the slot+constraint
 * API deliberately narrows values to plain Java types and erases that metadata.
 */
public final class ParameterExtractor {

    private final Arguments args;

    private ParameterExtractor(Arguments args) {
        this.args = Objects.requireNonNull(args, "args");
    }

    public static ParameterExtractor of(Arguments args) {
        return new ParameterExtractor(args);
    }

    public PositionalSlot positional(int index, String displayName) {
        return new PositionalSlot(index, displayName);
    }

    public NamedSlot named(String name) {
        return new NamedSlot(name);
    }

    /**
     * Varargs view of positional arguments from {@code startIndex} onward.
     * Returns {@link List#of()} when there are no remaining positionals.
     */
    public List<Object> varargsFrom(int startIndex) {
        List<Object> positional = args.positional();
        if (startIndex >= positional.size()) return List.of();
        return positional.subList(startIndex, positional.size());
    }

    public Field positionalFieldAt(int index) { return args.positionalFieldAt(index); }
    public ArrowType positionalTypeAt(int index) { return args.positionalTypeAt(index); }
    public int positionalCount() { return args.positional().size(); }
    public Arguments arguments() { return args; }

    // ---------- slots ----------

    public final class PositionalSlot {
        private final int index;
        private final String displayName;
        PositionalSlot(int index, String displayName) {
            this.index = index;
            this.displayName = displayName;
        }
        Object rawValue() {
            List<Object> p = args.positional();
            return index < p.size() ? p.get(index) : MISSING;
        }
        String name() { return displayName; }

        public LongConstraint asLong() { return new LongConstraint(rawValue(), name()); }
        public DoubleConstraint asDouble() { return new DoubleConstraint(rawValue(), name()); }
        public StringConstraint asString() { return new StringConstraint(rawValue(), name()); }
        public BoolConstraint asBool() { return new BoolConstraint(rawValue(), name()); }
    }

    public final class NamedSlot {
        private final String name;
        NamedSlot(String name) { this.name = name; }
        Object rawValue() {
            return args.named().containsKey(name) ? args.named().get(name) : MISSING;
        }

        public LongConstraint asLong() { return new LongConstraint(rawValue(), name); }
        public DoubleConstraint asDouble() { return new DoubleConstraint(rawValue(), name); }
        public StringConstraint asString() { return new StringConstraint(rawValue(), name); }
        public BoolConstraint asBool() { return new BoolConstraint(rawValue(), name); }
    }

    // ---------- constraint builders ----------

    /** Sentinel: argument missing entirely (positional past end / named key absent). */
    private static final Object MISSING = new Object();

    private static boolean present(Object raw) {
        return raw != MISSING && raw != null;
    }

    public static final class LongConstraint {
        private final Object raw;
        private final String name;
        private Long min, max;
        LongConstraint(Object raw, String name) { this.raw = raw; this.name = name; }

        public LongConstraint ge(long v) { this.min = v; return this; }
        public LongConstraint le(long v) { this.max = v; return this; }
        public LongConstraint between(long lo, long hi) { this.min = lo; this.max = hi; return this; }

        public long required() {
            if (!present(raw)) throw new IllegalArgumentException(name + " cannot be NULL");
            return validate(((Number) raw).longValue());
        }
        public long orElse(long defaultValue) {
            if (!present(raw)) return defaultValue;
            return validate(((Number) raw).longValue());
        }
        /**
         * Speculative-bind-time validator: rejects explicit NULL, tolerates
         * absence, applies range check if a value is present. Use in
         * {@code onBind} so catalog-discovery calls (which pass
         * {@link Arguments#empty()}) don't fail.
         */
        public void notNull() {
            if (raw == null) throw new IllegalArgumentException(name + " cannot be NULL");
            if (raw != MISSING) validate(((Number) raw).longValue());
        }
        private long validate(long v) {
            if (min != null && v < min) throw new IllegalArgumentException(name + " must be >= " + min + ", got " + v);
            if (max != null && v > max) throw new IllegalArgumentException(name + " must be <= " + max + ", got " + v);
            return v;
        }
    }

    public static final class DoubleConstraint {
        private final Object raw;
        private final String name;
        private Double min, max;
        private boolean allowNonFinite = false;
        DoubleConstraint(Object raw, String name) { this.raw = raw; this.name = name; }

        public DoubleConstraint ge(double v) { this.min = v; return this; }
        public DoubleConstraint le(double v) { this.max = v; return this; }
        public DoubleConstraint between(double lo, double hi) { this.min = lo; this.max = hi; return this; }
        public DoubleConstraint allowNonFinite() { this.allowNonFinite = true; return this; }

        public double required() {
            if (!present(raw)) throw new IllegalArgumentException(name + " cannot be NULL");
            return validate(coerce(raw));
        }
        public double orElse(double defaultValue) {
            if (!present(raw)) return defaultValue;
            return validate(coerce(raw));
        }
        /** See {@link LongConstraint#notNull()}. */
        public void notNull() {
            if (raw == null) throw new IllegalArgumentException(name + " cannot be NULL");
            if (raw != MISSING) validate(coerce(raw));
        }
        private double coerce(Object v) {
            if (v instanceof BigDecimal bd) return bd.doubleValue();
            return ((Number) v).doubleValue();
        }
        private double validate(double v) {
            if (!allowNonFinite && (Double.isNaN(v) || Double.isInfinite(v))) {
                throw new IllegalArgumentException(name + " must be a finite number");
            }
            if (min != null && v < min) throw new IllegalArgumentException(name + " must be >= " + min + ", got " + v);
            if (max != null && v > max) throw new IllegalArgumentException(name + " must be <= " + max + ", got " + v);
            return v;
        }
    }

    public static final class StringConstraint {
        private final Object raw;
        private final String name;
        private String[] allowed;
        private boolean nonEmpty = false;
        StringConstraint(Object raw, String name) { this.raw = raw; this.name = name; }

        public StringConstraint oneOf(String... values) { this.allowed = values; return this; }
        public StringConstraint nonEmpty() { this.nonEmpty = true; return this; }

        public String required() {
            if (!present(raw)) throw new IllegalArgumentException(name + " cannot be NULL");
            return validate(raw.toString());
        }
        public String orElse(String defaultValue) {
            if (!present(raw)) return defaultValue;
            return validate(raw.toString());
        }
        /** See {@link LongConstraint#notNull()}. */
        public void notNull() {
            if (raw == null) throw new IllegalArgumentException(name + " cannot be NULL");
            if (raw != MISSING) validate(raw.toString());
        }
        private String validate(String v) {
            if (nonEmpty && v.isEmpty()) throw new IllegalArgumentException(name + " must not be empty");
            if (allowed != null) {
                for (String a : allowed) if (a.equals(v)) return v;
                throw new IllegalArgumentException(
                    name + " must be one of the allowed choices " + Arrays.toString(allowed) + ", got '" + v + "'");
            }
            return v;
        }
    }

    public static final class BoolConstraint {
        private final Object raw;
        private final String name;
        BoolConstraint(Object raw, String name) { this.raw = raw; this.name = name; }

        public boolean required() {
            if (!present(raw)) throw new IllegalArgumentException(name + " cannot be NULL");
            return (Boolean) raw;
        }
        public boolean orElse(boolean defaultValue) {
            if (!present(raw)) return defaultValue;
            return (Boolean) raw;
        }
        /** See {@link LongConstraint#notNull()}. */
        public void notNull() {
            if (raw == null) throw new IllegalArgumentException(name + " cannot be NULL");
        }
    }
}
