// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/**
 * A value read from storage is not a valid {@link StorageEnvelope}: an empty
 * value, an unknown tag (such as a raw value written without the envelope), a
 * malformed pointer, or a pointer read by a process with no blob vendor
 * configured.
 */
public class StorageEnvelopeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Create the error.
     *
     * @param message what was wrong with the stored value
     */
    public StorageEnvelopeException(String message) {
        super(message);
    }
}
