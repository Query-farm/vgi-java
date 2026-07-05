// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

/**
 * Argument specification for a VGI function parameter. Mirrors vgi-go {@code ArgSpec}.
 *
 * <p>{@code anyType=true} declares an "any" parameter — DuckDB matches the
 * argument against any concrete type at bind time. {@code tableInput=true}
 * declares a TABLE-typed input (table-in-out functions). Both are reflected in
 * field metadata as {@code vgi_type=any} / {@code vgi_type=table}; the
 * {@code arrowType} field is a placeholder ({@code null} type) for both.
 *
 * <p>{@code children} carries the child {@link Field}s for nested Arrow types
 * (struct fields, list element type, fixed-size-list element type). Required
 * by the Arrow schema deserialiser on the C++ side for {@link ArrowType.List},
 * {@link ArrowType.FixedSizeList}, and {@link ArrowType.Struct}.
 *
 * @param name        SQL parameter name (the wire field name for kwarg-style calls).
 * @param position    zero-based positional slot, or {@code -1} for a named-only argument.
 * @param arrowType   declared Arrow type; a {@code null}/{@link ArrowType.Null} placeholder for "any" and TABLE inputs.
 * @param doc         human-readable parameter description.
 * @param isConst     {@code true} for a compile-time-constant (bind-validated) argument.
 * @param hasDefault  {@code true} when {@code defaultValue} applies (named-only arguments only).
 * @param defaultValue default literal used when {@code hasDefault}.
 * @param typeBound   bind-time predicates applied to an "any"-typed argument.
 * @param varargs     {@code true} when this argument absorbs a variable number of trailing positionals.
 * @param anyType     {@code true} for an "any" parameter ({@code vgi_type=any}).
 * @param tableInput  {@code true} for a TABLE-typed input ({@code vgi_type=table}).
 * @param children    child {@link Field}s for nested Arrow types; empty otherwise.
 * @param constraints discovery-facing validation constraints (choices / numeric
 *                    bounds / regex) surfaced through {@code vgi_function_arguments()};
 *                    {@link Constraints#NONE} when unconstrained.
 */
public record ArgSpec(
        String name,
        int position,
        ArrowType arrowType,
        String doc,
        boolean isConst,
        boolean hasDefault,
        String defaultValue,
        List<TypeBoundPredicate> typeBound,
        boolean varargs,
        boolean anyType,
        boolean tableInput,
        List<Field> children,
        Constraints constraints) {

    /** Canonical constructor: rejects positional + hasDefault (DuckDB's binder
     *  does not apply per-positional defaults — declaring one is dead metadata
     *  at the SQL call site; use {@link #named(String, ArrowType, String)} for
     *  defaultable kwarg-style arguments) and normalises a {@code null}
     *  {@code constraints} to {@link Constraints#NONE}. */
    public ArgSpec {
        if (position >= 0 && hasDefault) {
            throw new IllegalArgumentException(
                    "ArgSpec '" + name + "' at position " + position
                            + " cannot have hasDefault=true: DuckDB's binder does not apply"
                            + " positional defaults. Use a named-only ArgSpec (position=-1)"
                            + " if you need a default value.");
        }
        if (constraints == null) {
            constraints = Constraints.NONE;
        }
    }

    /**
     * Discovery-facing per-argument validation constraints, surfaced as Arrow
     * field metadata by {@code ArgumentSpecSerializer} and read by the C++
     * {@code vgi_function_arguments()} diagnostic. Mirrors the per-argument
     * constraint fields on vgi-python's {@code Param} / {@code ConstParam}.
     *
     * <p>Every field is optional ({@code null} = absent). The serializer encodes
     * them presence-only: {@code choices} → {@code vgi_choices} (JSON array),
     * {@code ge}/{@code le}/{@code gt}/{@code lt} → a single {@code vgi_range}
     * interval-notation string, {@code pattern} → {@code vgi_pattern} (raw regex).
     *
     * @param choices closed set of allowed values, or {@code null}.
     * @param ge      inclusive lower bound ({@code value >= ge}), or {@code null}.
     * @param le      inclusive upper bound ({@code value <= le}), or {@code null}.
     * @param gt      exclusive lower bound ({@code value > gt}), or {@code null}.
     * @param lt      exclusive upper bound ({@code value < lt}), or {@code null}.
     * @param pattern regex the value must match, or {@code null}.
     */
    public record Constraints(
            List<Object> choices,
            Number ge,
            Number le,
            Number gt,
            Number lt,
            String pattern) {

        /** The empty constraint set (every field absent). */
        public static final Constraints NONE =
                new Constraints(null, null, null, null, null, null);

        /** Defensive copy of {@code choices} so the record stays immutable. */
        public Constraints {
            if (choices != null) {
                choices = List.copyOf(choices);
            }
        }

        /** {@return {@code true} when no constraint at all is present}. */
        public boolean isEmpty() {
            return choices == null && ge == null && le == null && gt == null
                    && lt == null && (pattern == null || pattern.isEmpty());
        }

        /**
         * Numeric bounds only (no choices / pattern), any of which may be
         * {@code null}.
         *
         * @param ge inclusive lower bound, or {@code null}.
         * @param le inclusive upper bound, or {@code null}.
         * @param gt exclusive lower bound, or {@code null}.
         * @param lt exclusive upper bound, or {@code null}.
         * @return a bounds-only {@link Constraints}.
         */
        public static Constraints range(Number ge, Number le, Number gt, Number lt) {
            return new Constraints(null, ge, le, gt, lt, null);
        }

        /**
         * Closed-set constraint only.
         *
         * @param choices the allowed values.
         * @return a choices-only {@link Constraints}.
         */
        public static Constraints choices(List<Object> choices) {
            return new Constraints(choices, null, null, null, null, null);
        }

        /**
         * Regex constraint only.
         *
         * @param pattern the regex the value must match.
         * @return a pattern-only {@link Constraints}.
         */
        public static Constraints pattern(String pattern) {
            return new Constraints(null, null, null, null, null, pattern);
        }
    }

    /**
     * Full constructor with an empty {@code children} list (non-nested types).
     *
     * @param name         SQL parameter name.
     * @param position     zero-based positional slot, or {@code -1} for named-only.
     * @param arrowType    declared Arrow type.
     * @param doc          human-readable parameter description.
     * @param isConst      {@code true} for a compile-time-constant argument.
     * @param hasDefault   {@code true} when {@code defaultValue} applies (named-only).
     * @param defaultValue default literal used when {@code hasDefault}.
     * @param typeBound    bind-time predicates applied to an "any"-typed argument.
     * @param varargs      {@code true} when the argument absorbs trailing positionals.
     * @param anyType      {@code true} for an "any" parameter.
     * @param tableInput   {@code true} for a TABLE-typed input.
     */
    public ArgSpec(String name, int position, ArrowType arrowType, String doc,
                    boolean isConst, boolean hasDefault, String defaultValue,
                    List<TypeBoundPredicate> typeBound,
                    boolean varargs, boolean anyType, boolean tableInput) {
        this(name, position, arrowType, doc, isConst, hasDefault, defaultValue,
                typeBound, varargs, anyType, tableInput, List.of(), Constraints.NONE);
    }

    /**
     * Full constructor with explicit {@code children} but no constraints
     * (delegates with {@link Constraints#NONE}).
     *
     * @param name         SQL parameter name.
     * @param position     zero-based positional slot, or {@code -1} for named-only.
     * @param arrowType    declared Arrow type.
     * @param doc          human-readable parameter description.
     * @param isConst      {@code true} for a compile-time-constant argument.
     * @param hasDefault   {@code true} when {@code defaultValue} applies (named-only).
     * @param defaultValue default literal used when {@code hasDefault}.
     * @param typeBound    bind-time predicates applied to an "any"-typed argument.
     * @param varargs      {@code true} when the argument absorbs trailing positionals.
     * @param anyType      {@code true} for an "any" parameter.
     * @param tableInput   {@code true} for a TABLE-typed input.
     * @param children     child {@link Field}s for nested Arrow types.
     */
    public ArgSpec(String name, int position, ArrowType arrowType, String doc,
                    boolean isConst, boolean hasDefault, String defaultValue,
                    List<TypeBoundPredicate> typeBound,
                    boolean varargs, boolean anyType, boolean tableInput,
                    List<Field> children) {
        this(name, position, arrowType, doc, isConst, hasDefault, defaultValue,
                typeBound, varargs, anyType, tableInput, children, Constraints.NONE);
    }

    /**
     * Return a copy of this spec carrying {@code newConstraints} (a {@code null}
     * argument normalises to {@link Constraints#NONE}). All other components are
     * preserved.
     *
     * @param newConstraints the discovery constraints to attach.
     * @return a new {@link ArgSpec} with the given constraints.
     */
    public ArgSpec withConstraints(Constraints newConstraints) {
        return new ArgSpec(name, position, arrowType, doc, isConst, hasDefault,
                defaultValue, typeBound, varargs, anyType, tableInput, children,
                newConstraints == null ? Constraints.NONE : newConstraints);
    }

    /**
     * Minimal positional runtime-column argument (non-const, no doc, no bounds).
     *
     * @param name      SQL parameter name.
     * @param position  zero-based positional slot.
     * @param arrowType declared Arrow type.
     */
    public ArgSpec(String name, int position, ArrowType arrowType) {
        this(name, position, arrowType, "", false, false, "", List.of(), false, false, false);
    }

    /**
     * Positional argument with explicit const flag.
     *
     * @param name      SQL parameter name.
     * @param position  zero-based positional slot.
     * @param arrowType declared Arrow type.
     * @param isConst   {@code true} for a compile-time-constant argument.
     */
    public ArgSpec(String name, int position, ArrowType arrowType, boolean isConst) {
        this(name, position, arrowType, "", isConst, false, "", List.of(), false, false, false);
    }

    /**
     * Constructor for the non-table case ({@code tableInput=false}).
     *
     * @param name         SQL parameter name.
     * @param position     zero-based positional slot, or {@code -1} for named-only.
     * @param arrowType    declared Arrow type.
     * @param doc          human-readable parameter description.
     * @param isConst      {@code true} for a compile-time-constant argument.
     * @param hasDefault   {@code true} when {@code defaultValue} applies (named-only).
     * @param defaultValue default literal used when {@code hasDefault}.
     * @param typeBound    bind-time predicates applied to an "any"-typed argument.
     * @param varargs      {@code true} when the argument absorbs trailing positionals.
     * @param anyType      {@code true} for an "any" parameter.
     */
    public ArgSpec(String name, int position, ArrowType arrowType, String doc, boolean isConst,
                    boolean hasDefault, String defaultValue, List<TypeBoundPredicate> typeBound,
                    boolean varargs, boolean anyType) {
        this(name, position, arrowType, doc, isConst, hasDefault, defaultValue, typeBound,
                varargs, anyType, false);
    }

    /**
     * "Any"-typed positional argument matched against {@code typeBound} at bind time.
     *
     * @param name      parameter name.
     * @param position  positional slot.
     * @param typeBound bind-time predicates the matched type must satisfy.
     * @return an any-typed {@link ArgSpec}.
     */
    public static ArgSpec any(String name, int position, List<TypeBoundPredicate> typeBound) {
        return new ArgSpec(name, position, new ArrowType.Null(), "", false, false, "",
                typeBound, false, true, false);
    }

    /**
     * TABLE-typed positional input, for table-in-out functions.
     *
     * @param name     parameter name.
     * @param position positional slot.
     * @return a TABLE-input {@link ArgSpec}.
     */
    public static ArgSpec table(String name, int position) {
        return new ArgSpec(name, position, new ArrowType.Null(), "", false, false, "",
                List.of(), false, false, true);
    }

    /**
     * Named-only constant argument (no positional slot, accessible only via
     * {@code arg => value} syntax). The most common shape for fixture
     * configuration knobs like {@code batch_size}, {@code logging}, etc.
     *
     * @param name         parameter name (the kwarg key).
     * @param type         Arrow type of the constant.
     * @param defaultValue default literal used when the kwarg is omitted.
     * @return a named-only {@link ArgSpec}.
     */
    public static ArgSpec named(String name, ArrowType type, String defaultValue) {
        return new ArgSpec(name, -1, type, "", true, true, defaultValue,
                List.of(), false, false, false);
    }

    /**
     * Plain positional const argument with no default value.
     *
     * @param name     parameter name.
     * @param position positional slot.
     * @param type     Arrow type of the constant.
     * @return a positional const {@link ArgSpec}.
     */
    public static ArgSpec positional(String name, int position, ArrowType type) {
        return new ArgSpec(name, position, type, "", true, false, "",
                List.of(), false, false, false);
    }

    /**
     * Positional varargs constant argument (e.g. {@code make_pairs(a, b, c, d, ...)}).
     *
     * @param name     parameter name.
     * @param position positional slot the varargs begin at.
     * @param type     Arrow type each vararg element takes.
     * @return a varargs const {@link ArgSpec}.
     */
    public static ArgSpec varargs(String name, int position, ArrowType type) {
        return new ArgSpec(name, position, type, "", true, false, "",
                List.of(), true, false, false);
    }

    /**
     * Construct an ArgSpec for a nested Arrow type (struct/list/fixed_list)
     * with explicit child field shape. {@code varargs} switches the spec to
     * varargs.
     *
     * @param name      parameter name.
     * @param position  positional slot.
     * @param arrowType nested Arrow type (struct/list/fixed-size-list).
     * @param children  child {@link Field}s describing the nested shape.
     * @param varargs   {@code true} to make the nested argument variadic.
     * @return a nested-type {@link ArgSpec}.
     */
    public static ArgSpec nested(String name, int position, ArrowType arrowType,
                                   List<Field> children, boolean varargs) {
        return new ArgSpec(name, position, arrowType, "", false, false, "",
                List.of(), varargs, false, false, children);
    }
}
