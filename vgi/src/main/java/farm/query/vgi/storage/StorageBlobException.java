// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/**
 * An externalized storage value's blob could not be read.
 *
 * <p>The message starts {@code storage blob <object id>}, following the
 * SDK-neutral contract in the vgi extension's
 * {@code docs/testing/blob-test-catalog.md}:</p>
 * <ul>
 *   <li>{@link StorageBlobIntegrityException}: {@code storage blob <id> failed
 *       its integrity check: sha256 mismatch} (or {@code size mismatch ...});</li>
 *   <li>{@link StorageBlobMissingException}: {@code storage blob <id> is
 *       missing: not found in the blob vendor (expired or deleted?)};</li>
 *   <li>this class: {@code storage blob <id> could not be read: <detail>}.</li>
 * </ul>
 */
public class StorageBlobException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Create the error.
     *
     * @param message the contract message, starting {@code storage blob <id>}
     */
    public StorageBlobException(String message) {
        super(message);
    }
}
