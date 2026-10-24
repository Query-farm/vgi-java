// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.HasErrorCode;

/**
 * A lookup by name failed: no such function, table, schema, catalog, or other object.
 *
 * <p>Reported on the wire with {@code error_code = NOT_FOUND} (vgi-rpc error model, WIRE_PROTOCOL
 * §8), so a client can tell this failure from a worker bug. It extends
 * {@link IllegalArgumentException}, the JDK type the SDK threw here before, so existing {@code catch} clauses
 * keep working. No {@code error_kind} is set.
 */
public class NotFoundException extends IllegalArgumentException implements HasErrorCode {

    private static final long serialVersionUID = 1L;

    /**
     * Build the error.
     *
     * @param message the human-readable message, reported verbatim
     */
    public NotFoundException(String message) {
        super(message);
    }

    /**
     * Build the error with an underlying cause.
     *
     * @param message the human-readable message, reported verbatim
     * @param cause the underlying failure
     */
    public NotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    /** @return {@link Code#NOT_FOUND} */
    @Override
    public Code errorCode() {
        return Code.NOT_FOUND;
    }
}
