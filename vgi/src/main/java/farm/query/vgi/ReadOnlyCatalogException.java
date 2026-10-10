// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.HasErrorCode;

/**
 * A write (DDL or DML) was sent to a catalog that is read-only.
 *
 * <p>Reported on the wire with {@code error_code = FAILED_PRECONDITION} (vgi-rpc error model, WIRE_PROTOCOL
 * §8), so a client can tell this failure from a worker bug. It extends
 * {@link UnsupportedOperationException}, the JDK type the SDK threw here before, so existing {@code catch} clauses
 * keep working. No {@code error_kind} is set.
 */
public class ReadOnlyCatalogException extends UnsupportedOperationException implements HasErrorCode {

    private static final long serialVersionUID = 1L;

    /**
     * Build the error.
     *
     * @param message the human-readable message, reported verbatim
     */
    public ReadOnlyCatalogException(String message) {
        super(message);
    }

    /**
     * Build the error with an underlying cause.
     *
     * @param message the human-readable message, reported verbatim
     * @param cause the underlying failure
     */
    public ReadOnlyCatalogException(String message, Throwable cause) {
        super(message, cause);
    }

    /** @return {@link Code#FAILED_PRECONDITION} */
    @Override
    public Code errorCode() {
        return Code.FAILED_PRECONDITION;
    }
}
