// Copyright 2025-2026 Query.Farm LLC

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
    String value() default "";
    String doc() default "";
}
