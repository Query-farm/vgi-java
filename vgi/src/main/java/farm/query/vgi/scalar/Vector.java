// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import farm.query.vgi.function.TypeBoundPredicate;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@code compute()} parameter as a per-row input column (vector arg).
 * The Java type of the parameter must be a concrete Arrow vector class
 * ({@code BigIntVector}, {@code VarCharVector}, {@code Float8Vector}, …); the
 * Arrow type for the {@link farm.query.vgi.function.FunctionSpec} entry is
 * inferred from it.
 *
 * <p>Equivalent to vgi-python's {@code Annotated[pa.Int64Array, Param(doc=...)]}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Vector {
    /** {@return the wire name used in the generated {@code ArgSpec}; the empty
     *  default means the Java parameter name, snake-cased} */
    String value() default "";
    /** {@return human-readable documentation carried into the function spec
     *  and surfaced by catalog introspection; empty for none} */
    String doc() default "";
    /** {@return whether this input is any-typed — the spec accepts any Arrow
     *  type instead of inferring one from the parameter class. The Java type
     *  must then be the generic {@code FieldVector}; pair with
     *  {@link #typeBound()} to constrain at bind time} */
    boolean any() default false;
    /** {@return whether this input is varargs — it consumes all remaining
     *  positional input columns from this position. The Java type must be
     *  {@code List<FieldVector>} (or a {@code List} of a typed vector subtype
     *  to constrain the element type)} */
    boolean varargs() default false;
    /** {@return type-bound predicates enforced at bind time for any-typed
     *  inputs, reported as a function-named SQL-typed error on violation;
     *  empty means unconstrained} */
    TypeBoundPredicate[] typeBound() default {};
    /** {@return the closed set of allowed values surfaced for agent discovery
     *  ({@code vgi_choices}); empty means no restriction. Declared as strings —
     *  workers needing non-string choices attach an
     *  {@link farm.query.vgi.function.ArgSpec.Constraints} directly} */
    String[] choices() default {};
    /** {@return inclusive lower bound ({@code value >= ge}) for agent discovery
     *  ({@code vgi_range}); {@link Double#NaN} means unbounded} */
    double ge() default Double.NaN;
    /** {@return inclusive upper bound ({@code value <= le}); {@link Double#NaN}
     *  means unbounded} */
    double le() default Double.NaN;
    /** {@return exclusive lower bound ({@code value > gt}); {@link Double#NaN}
     *  means unbounded} */
    double gt() default Double.NaN;
    /** {@return exclusive upper bound ({@code value < lt}); {@link Double#NaN}
     *  means unbounded} */
    double lt() default Double.NaN;
    /** {@return regex the value must match ({@code vgi_pattern}); empty for none} */
    String pattern() default "";
}
