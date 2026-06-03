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
    /** Override the wire name; default uses the parameter name (snake-cased). */
    String value() default "";
    /** Human-readable documentation surfaced in the function spec. */
    String doc() default "";
    /** Marks an any-typed input — accepts any Arrow type. Parameter Java type
     *  must be a generic vector reference ({@code FieldVector}). */
    boolean any() default false;
    /** Varargs: parameter must be {@code List<FieldVector>} (or a typed
     *  vector subtype to constrain element type). Consumes all remaining
     *  positional input columns from this position. */
    boolean varargs() default false;
    /** Type-bound predicate applied at bind time for any-typed inputs. Empty = no bound. */
    TypeBoundPredicate[] typeBound() default {};
}
