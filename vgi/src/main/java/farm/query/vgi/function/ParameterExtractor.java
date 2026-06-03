// Copyright 2026 Query Farm LLC - https://query.farm

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

    /**
     * Wrap an {@link Arguments} snapshot.
     *
     * @param args the parsed arguments.
     * @return a new extractor.
     */
    public static ParameterExtractor of(Arguments args) {
        return new ParameterExtractor(args);
    }

    /**
     * Begin extracting the positional argument at {@code index}.
     *
     * @param index       zero-based positional index.
     * @param displayName name used in validation error messages.
     * @return a positional slot.
     */
    public PositionalSlot positional(int index, String displayName) {
        return new PositionalSlot(index, displayName);
    }

    /**
     * Begin extracting the named (kwarg) argument {@code name}.
     *
     * @param name the kwarg name (also used in error messages).
     * @return a named slot.
     */
    public NamedSlot named(String name) {
        return new NamedSlot(name);
    }

    /**
     * Varargs view of positional arguments from {@code startIndex} onward.
     * Returns {@link List#of()} when there are no remaining positionals.
     *
     * @param startIndex first positional index to include.
     * @return the trailing positional values.
     */
    public List<Object> varargsFrom(int startIndex) {
        List<Object> positional = args.positional();
        if (startIndex >= positional.size()) return List.of();
        return positional.subList(startIndex, positional.size());
    }

    /** Source {@link Field} for the positional argument at {@code index} (lossless type metadata).
     *
     *  @param index zero-based positional index.
     *  @return the source field, or {@code null} when not available. */
    public Field positionalFieldAt(int index) { return args.positionalFieldAt(index); }

    /** Source Arrow type for the positional argument at {@code index}.
     *
     *  @param index zero-based positional index.
     *  @return the Arrow type, or {@code null} when not available. */
    public ArrowType positionalTypeAt(int index) { return args.positionalTypeAt(index); }

    /** Number of positional arguments supplied.
     *
     *  @return the positional count. */
    public int positionalCount() { return args.positional().size(); }

    /** The wrapped raw arguments.
     *
     *  @return the underlying {@link Arguments}. */
    public Arguments arguments() { return args; }

    // ---------- slots ----------

    /** A selected positional argument awaiting a type constraint. */
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

        /** Interpret the value as a long.
         *  @return a {@link LongConstraint}. */
        public LongConstraint asLong() { return new LongConstraint(rawValue(), name()); }
        /** Interpret the value as a double.
         *  @return a {@link DoubleConstraint}. */
        public DoubleConstraint asDouble() { return new DoubleConstraint(rawValue(), name()); }
        /** Interpret the value as a string.
         *  @return a {@link StringConstraint}. */
        public StringConstraint asString() { return new StringConstraint(rawValue(), name()); }
        /** Interpret the value as a boolean.
         *  @return a {@link BoolConstraint}. */
        public BoolConstraint asBool() { return new BoolConstraint(rawValue(), name()); }
    }

    /** A selected named (kwarg) argument awaiting a type constraint. */
    public final class NamedSlot {
        private final String name;
        NamedSlot(String name) { this.name = name; }
        Object rawValue() {
            return args.named().containsKey(name) ? args.named().get(name) : MISSING;
        }

        /** Interpret the value as a long.
         *  @return a {@link LongConstraint}. */
        public LongConstraint asLong() { return new LongConstraint(rawValue(), name); }
        /** Interpret the value as a double.
         *  @return a {@link DoubleConstraint}. */
        public DoubleConstraint asDouble() { return new DoubleConstraint(rawValue(), name); }
        /** Interpret the value as a string.
         *  @return a {@link StringConstraint}. */
        public StringConstraint asString() { return new StringConstraint(rawValue(), name); }
        /** Interpret the value as a boolean.
         *  @return a {@link BoolConstraint}. */
        public BoolConstraint asBool() { return new BoolConstraint(rawValue(), name); }
    }

    // ---------- constraint builders ----------

    /** Sentinel: argument missing entirely (positional past end / named key absent). */
    private static final Object MISSING = new Object();

    private static boolean present(Object raw) {
        return raw != MISSING && raw != null;
    }

    /** A long-valued constraint with optional inclusive range bounds. */
    public static final class LongConstraint {
        private final Object raw;
        private final String name;
        private Long min, max;
        LongConstraint(Object raw, String name) { this.raw = raw; this.name = name; }

        /** Require value {@code >= v}.
         *  @param v inclusive lower bound.
         *  @return this constraint. */
        public LongConstraint ge(long v) { this.min = v; return this; }
        /** Require value {@code <= v}.
         *  @param v inclusive upper bound.
         *  @return this constraint. */
        public LongConstraint le(long v) { this.max = v; return this; }
        /** Require value within {@code [lo, hi]} (inclusive both ends).
         *  @param lo inclusive lower bound.
         *  @param hi inclusive upper bound.
         *  @return this constraint. */
        public LongConstraint between(long lo, long hi) { this.min = lo; this.max = hi; return this; }

        /** Resolve, throwing when the argument is missing or null.
         *  @return the validated value.
         *  @throws IllegalArgumentException when missing/null or out of range. */
        public long required() {
            if (!present(raw)) throw new IllegalArgumentException(name + " cannot be NULL");
            return validate(((Number) raw).longValue());
        }
        /** Resolve, substituting {@code defaultValue} when missing or null.
         *  @param defaultValue value used when the argument is absent.
         *  @return the validated value, or {@code defaultValue}.
         *  @throws IllegalArgumentException when a present value is out of range. */
        public long orElse(long defaultValue) {
            if (!present(raw)) return defaultValue;
            return validate(((Number) raw).longValue());
        }
        /**
         * Speculative-bind-time validator: rejects explicit NULL, tolerates
         * absence, applies range check if a value is present. Use in
         * {@code onBind} so catalog-discovery calls (which pass
         * {@link Arguments#empty()}) don't fail.
         *
         * @throws IllegalArgumentException when the value is explicitly NULL or out of range.
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

    /** A double-valued constraint; rejects NaN/±Inf unless {@link #allowNonFinite()}. */
    public static final class DoubleConstraint {
        private final Object raw;
        private final String name;
        private Double min, max;
        private boolean allowNonFinite = false;
        DoubleConstraint(Object raw, String name) { this.raw = raw; this.name = name; }

        /** Require value {@code >= v}.
         *  @param v inclusive lower bound.
         *  @return this constraint. */
        public DoubleConstraint ge(double v) { this.min = v; return this; }
        /** Require value {@code <= v}.
         *  @param v inclusive upper bound.
         *  @return this constraint. */
        public DoubleConstraint le(double v) { this.max = v; return this; }
        /** Require value within {@code [lo, hi]} (inclusive both ends).
         *  @param lo inclusive lower bound.
         *  @param hi inclusive upper bound.
         *  @return this constraint. */
        public DoubleConstraint between(double lo, double hi) { this.min = lo; this.max = hi; return this; }
        /** Permit NaN and infinite values, which are otherwise rejected.
         *  @return this constraint. */
        public DoubleConstraint allowNonFinite() { this.allowNonFinite = true; return this; }

        /** Resolve, throwing when the argument is missing or null.
         *  @return the validated value.
         *  @throws IllegalArgumentException when missing/null, non-finite, or out of range. */
        public double required() {
            if (!present(raw)) throw new IllegalArgumentException(name + " cannot be NULL");
            return validate(coerce(raw));
        }
        /** Resolve, substituting {@code defaultValue} when missing or null.
         *  @param defaultValue value used when the argument is absent.
         *  @return the validated value, or {@code defaultValue}.
         *  @throws IllegalArgumentException when a present value is non-finite or out of range. */
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

    /** A string-valued constraint with optional allowed-set and non-empty checks. */
    public static final class StringConstraint {
        private final Object raw;
        private final String name;
        private String[] allowed;
        private boolean nonEmpty = false;
        StringConstraint(Object raw, String name) { this.raw = raw; this.name = name; }

        /** Restrict to one of the given values.
         *  @param values allowed string values.
         *  @return this constraint. */
        public StringConstraint oneOf(String... values) { this.allowed = values; return this; }
        /** Reject the empty string.
         *  @return this constraint. */
        public StringConstraint nonEmpty() { this.nonEmpty = true; return this; }

        /** Resolve, throwing when the argument is missing or null.
         *  @return the validated value.
         *  @throws IllegalArgumentException when missing/null or failing a constraint. */
        public String required() {
            if (!present(raw)) throw new IllegalArgumentException(name + " cannot be NULL");
            return validate(raw.toString());
        }
        /** Resolve, substituting {@code defaultValue} when missing or null.
         *  @param defaultValue value used when the argument is absent.
         *  @return the validated value, or {@code defaultValue}.
         *  @throws IllegalArgumentException when a present value fails a constraint. */
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

    /** A boolean-valued constraint. */
    public static final class BoolConstraint {
        private final Object raw;
        private final String name;
        BoolConstraint(Object raw, String name) { this.raw = raw; this.name = name; }

        /** Resolve, throwing when the argument is missing or null.
         *  @return the value.
         *  @throws IllegalArgumentException when missing or null. */
        public boolean required() {
            if (!present(raw)) throw new IllegalArgumentException(name + " cannot be NULL");
            return (Boolean) raw;
        }
        /** Resolve, substituting {@code defaultValue} when missing or null.
         *  @param defaultValue value used when the argument is absent.
         *  @return the value, or {@code defaultValue}. */
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
