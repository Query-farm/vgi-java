// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@code compute()} parameter as a const positional arg (bind-time
 * constant). The Java type drives Arrow type inference:
 * {@code long → INT64}, {@code double → FLOAT64}, {@code String → UTF8},
 * {@code boolean → BOOL}.
 *
 * <p>Equivalent to vgi-python's {@code Annotated[int, ConstParam(doc=...)]}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Const {
    /** {@return the wire name used in the generated {@code ArgSpec}; the empty
     *  default means the Java parameter name, snake-cased} */
    String value() default "";
    /** {@return human-readable documentation carried into the function spec
     *  and surfaced by catalog introspection; empty for none} */
    String doc() default "";
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
