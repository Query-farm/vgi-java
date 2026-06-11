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
    /** {@return the session-setting key looked up at process time; the empty
     *  default means the Java parameter name, snake-cased} */
    String value() default "";
    /** {@return the fallback used when the session doesn't supply the setting,
     *  parsed into the parameter's Java type; with the empty default an absent
     *  setting resolves to the type's zero value ({@code 0}, {@code false},
     *  {@code null})} */
    String default_() default "";
}
