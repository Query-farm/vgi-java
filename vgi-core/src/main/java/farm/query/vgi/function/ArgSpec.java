// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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

    public ArgSpec(String name, int position, ArrowType arrowType, String doc,
                    boolean isConst, boolean hasDefault, String defaultValue,
                    List<TypeBoundPredicate> typeBound,
                    boolean varargs, boolean anyType, boolean tableInput) {
        this(name, position, arrowType, doc, isConst, hasDefault, defaultValue,
                typeBound, varargs, anyType, tableInput, List.of());
    }

    public ArgSpec(String name, int position, ArrowType arrowType) {
        this(name, position, arrowType, "", false, false, "", List.of(), false, false, false);
    }

    public ArgSpec(String name, int position, ArrowType arrowType, boolean isConst) {
        this(name, position, arrowType, "", isConst, false, "", List.of(), false, false, false);
    }

    public ArgSpec(String name, int position, ArrowType arrowType, String doc, boolean isConst,
                    boolean hasDefault, String defaultValue, List<TypeBoundPredicate> typeBound,
                    boolean varargs, boolean anyType) {
        this(name, position, arrowType, doc, isConst, hasDefault, defaultValue, typeBound,
                varargs, anyType, false);
    }

    public static ArgSpec any(String name, int position, List<TypeBoundPredicate> typeBound) {
        return new ArgSpec(name, position, new ArrowType.Null(), "", false, false, "",
                typeBound, false, true, false);
    }

    public static ArgSpec table(String name, int position) {
        return new ArgSpec(name, position, new ArrowType.Null(), "", false, false, "",
                List.of(), false, false, true);
    }

    /**
     * Named-only constant argument (no positional slot, accessible only via
     * {@code arg => value} syntax). The most common shape for fixture
     * configuration knobs like {@code batch_size}, {@code logging}, etc.
     */
    public static ArgSpec named(String name, ArrowType type, String defaultValue) {
        return new ArgSpec(name, -1, type, "", true, true, defaultValue,
                List.of(), false, false, false);
    }

    /** Plain positional const argument with no default value. */
    public static ArgSpec positional(String name, int position, ArrowType type) {
        return new ArgSpec(name, position, type, "", true, false, "",
                List.of(), false, false, false);
    }

    /** Positional const argument with a default value (used by fixtures
     *  where the positional slot may be omitted at the call site). */
    public static ArgSpec positionalWithDefault(String name, int position, ArrowType type,
                                                  String defaultValue) {
        return new ArgSpec(name, position, type, "", true, true, defaultValue,
                List.of(), false, false, false);
    }

    /** Positional varargs constant argument (e.g. {@code make_pairs(a, b, c, d, ...)}). */
    public static ArgSpec varargs(String name, int position, ArrowType type) {
        return new ArgSpec(name, position, type, "", true, false, "",
                List.of(), true, false, false);
    }

    /**
     * Construct an ArgSpec for a nested Arrow type (struct/list/fixed_list)
     * with explicit child field shape. {@code varargs} switches the spec to
     * varargs.
     */
    public static ArgSpec nested(String name, int position, ArrowType arrowType,
                                   List<Field> children, boolean varargs) {
        return new ArgSpec(name, position, arrowType, "", false, false, "",
                List.of(), varargs, false, false, children);
    }
}
