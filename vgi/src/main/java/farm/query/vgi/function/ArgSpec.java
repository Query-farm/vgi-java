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
        List<Field> children) {

    /** Canonical constructor: rejects positional + hasDefault. DuckDB's binder
     *  does not apply per-positional defaults — declaring one is dead metadata
     *  at the SQL call site. Use {@link #named(String, ArrowType, String)} for
     *  defaultable arguments (kwarg-style {@code arg => value}). */
    public ArgSpec {
        if (position >= 0 && hasDefault) {
            throw new IllegalArgumentException(
                    "ArgSpec '" + name + "' at position " + position
                            + " cannot have hasDefault=true: DuckDB's binder does not apply"
                            + " positional defaults. Use a named-only ArgSpec (position=-1)"
                            + " if you need a default value.");
        }
    }

    /** Full constructor with an empty {@code children} list (non-nested types). */
    public ArgSpec(String name, int position, ArrowType arrowType, String doc,
                    boolean isConst, boolean hasDefault, String defaultValue,
                    List<TypeBoundPredicate> typeBound,
                    boolean varargs, boolean anyType, boolean tableInput) {
        this(name, position, arrowType, doc, isConst, hasDefault, defaultValue,
                typeBound, varargs, anyType, tableInput, List.of());
    }

    /** Minimal positional runtime-column argument (non-const, no doc, no bounds). */
    public ArgSpec(String name, int position, ArrowType arrowType) {
        this(name, position, arrowType, "", false, false, "", List.of(), false, false, false);
    }

    /** Positional argument with explicit const flag. */
    public ArgSpec(String name, int position, ArrowType arrowType, boolean isConst) {
        this(name, position, arrowType, "", isConst, false, "", List.of(), false, false, false);
    }

    /** Constructor for the non-table case ({@code tableInput=false}). */
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
