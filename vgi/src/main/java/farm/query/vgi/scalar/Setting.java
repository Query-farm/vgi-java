// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@code compute()} parameter as a session setting. Same type-mapping
 * rules as {@link Const}.
 *
 * <p>Equivalent to vgi-python's {@code Annotated[..., Setting("key")]}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Setting {
    /** Setting key on the wire. Default = parameter name (snake-cased). */
    String value() default "";
    /** Default if absent (parsed per parameter type). */
    String default_() default "";
}
