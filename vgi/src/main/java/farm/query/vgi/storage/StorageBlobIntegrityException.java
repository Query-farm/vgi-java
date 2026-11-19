// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/** The downloaded blob does not match the pointer's size or SHA-256. */
public class StorageBlobIntegrityException extends StorageBlobException {

    private static final long serialVersionUID = 1L;

    /**
     * Create the error.
     *
     * @param message the contract message
     */
    public StorageBlobIntegrityException(String message) {
        super(message);
    }
}
