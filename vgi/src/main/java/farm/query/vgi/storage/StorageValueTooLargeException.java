// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/** A value is too large for the backend and no blob vendor is configured. */
public class StorageValueTooLargeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Create the error.
     *
     * @param message the size, the backend's limit, and how to lift it
     */
    public StorageValueTooLargeException(String message) {
        super(message);
    }
}
