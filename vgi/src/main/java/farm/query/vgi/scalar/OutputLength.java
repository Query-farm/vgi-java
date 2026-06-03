// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Inject the batch row count. Use for functions that emit values driven by
 * row index (e.g. {@code random_bytes(seed, length)} — no vector input).
 * Parameter type must be {@code int}.
 *
 * <p>Equivalent to vgi-python's {@code Annotated[int, OutputLength()]}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface OutputLength {}
