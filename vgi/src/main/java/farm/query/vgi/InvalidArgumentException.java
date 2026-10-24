// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.HasErrorCode;

/**
 * The caller's input was wrong: an argument failed validation, had a bad value, or an argument or input column has a type the function rejects.
 *
 * <p>Reported on the wire with {@code error_code = INVALID_ARGUMENT} (vgi-rpc error model, WIRE_PROTOCOL
 * §8), so a client can tell this failure from a worker bug. It extends
 * {@link IllegalArgumentException}, the JDK type the SDK threw here before, so existing {@code catch} clauses
 * keep working. No {@code error_kind} is set.
 */
public class InvalidArgumentException extends IllegalArgumentException implements HasErrorCode {

    private static final long serialVersionUID = 1L;

    /**
     * Build the error.
     *
     * @param message the human-readable message, reported verbatim
     */
    public InvalidArgumentException(String message) {
        super(message);
    }

    /**
     * Build the error with an underlying cause.
     *
     * @param message the human-readable message, reported verbatim
     * @param cause the underlying failure
     */
    public InvalidArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    /** @return {@link Code#INVALID_ARGUMENT} */
    @Override
    public Code errorCode() {
        return Code.INVALID_ARGUMENT;
    }
}
