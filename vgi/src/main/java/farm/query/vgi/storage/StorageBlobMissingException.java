// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/** The blob is gone from the vendor or its storage (expired, deleted, never uploaded). */
public class StorageBlobMissingException extends StorageBlobException {

    private static final long serialVersionUID = 1L;

    /**
     * Create the error.
     *
     * @param message the contract message
     */
    public StorageBlobMissingException(String message) {
        super(message);
    }
}
