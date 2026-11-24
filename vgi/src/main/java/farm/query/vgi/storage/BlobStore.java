// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.storage;

import farm.query.vgirpc.external.ObjectMissingException;
import farm.query.vgirpc.external.ObjectStore;
import farm.query.vgirpc.vendor.BlobRef;
import farm.query.vgirpc.vendor.BlobScope;
import farm.query.vgirpc.vendor.VendorBlobClient;
import farm.query.vgirpc.vendor.VendorError;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * Where {@link ExternalizingStorage} keeps values too large for its backend: the blob
 * vendor ({@link #of(VendorBlobClient)}) or the worker's own bucket through an
 * {@link ObjectStore} ({@link #of(ObjectStore, String, long, AutoCloseable, java.util.function.Consumer)}, direct S3 / GCS).
 *
 * <p>Reads are verified against the size and SHA-256 in the {@link BlobRef}: a mismatch is a
 * {@link VendorError} with code {@link VendorError#SIZE_MISMATCH} or
 * {@link VendorError#SHA256_MISMATCH}, a missing object one with status 404.</p>
 */
public interface BlobStore extends AutoCloseable {

    /**
     * Store {@code data}.
     *
     * @param data           the bytes
     * @param scope          the execution the blob belongs to (for scoped clean-up), or {@code null}
     * @param idempotencyKey makes a retried write reuse the object (the vendor only), or {@code null}
     * @return the reference
     */
    BlobRef putBytes(byte[] data, BlobScope scope, String idempotencyKey);

    /**
     * Fetch and verify a blob.
     *
     * @param ref the reference
     * @return the bytes
     * @throws VendorError if the blob is missing, unreadable or does not match {@code ref}
     */
    byte[] getBytes(BlobRef ref);

    /**
     * Delete blobs by reference (a no-op for stores the worker never deletes from).
     *
     * @param refs the references
     */
    void deleteRefs(List<BlobRef> refs);

    /**
     * Delete every blob of one execution (a no-op for stores the worker never deletes from).
     *
     * @param shard       the shard
     * @param executionId the execution
     */
    void deleteScope(String shard, String executionId);

    /** Release the underlying client. */
    @Override
    void close();

    /**
     * The blob vendor.
     *
     * @param client the vendor client (purpose {@code fs})
     * @return the store; closing it closes {@code client}
     */
    static BlobStore of(VendorBlobClient client) {
        return new BlobStore() {
            @Override public BlobRef putBytes(byte[] d, BlobScope s, String k) { return client.putBytes(d, s, k); }
            @Override public byte[] getBytes(BlobRef ref) { return client.getBytes(ref); }
            @Override public void deleteRefs(List<BlobRef> refs) { client.deleteRefs(refs); }
            @Override public void deleteScope(String shard, String id) { client.deleteScope(shard, id); }
            @Override public void close() { client.close(); }
        };
    }

    /**
     * The worker's own bucket. {@link #deleteRefs} deletes best effort (failures ignored; a
     * bucket lifecycle rule, 2 days on {@code fs/}, removes leftovers); {@link #deleteScope}
     * does nothing (an object store cannot delete by scope); idempotency keys are ignored.
     *
     * <p>The blob's object id (stored in the value envelope, whose charset has no {@code /}) is
     * the object key without {@code keyPrefix}; reads put the prefix back.</p>
     *
     * @param store         the object store
     * @param keyPrefix     the prefix the store writes keys under (stripped from object ids)
     * @param maxFetchBytes the largest blob read back
     * @param closer        closed by {@link #close()}, or {@code null}
     * @param deleter       best-effort delete of one object key, or {@code null}
     * @return the store
     */
    static BlobStore of(ObjectStore store, String keyPrefix, long maxFetchBytes, AutoCloseable closer,
                        java.util.function.Consumer<String> deleter) {
        return new BlobStore() {
            @Override
            public BlobRef putBytes(byte[] data, BlobScope scope, String idempotencyKey) {
                try {
                    String key = store.putObject(data);
                    if (!key.startsWith(keyPrefix)) {
                        throw new VendorError("object key outside the storage prefix: " + key);
                    }
                    return new BlobRef(key.substring(keyPrefix.length()), data.length, sha256Hex(data));
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new VendorError("blob write failed (" + e.getClass().getSimpleName() + ")");
                }
            }

            @Override
            public byte[] getBytes(BlobRef ref) {
                if (ref.size() > maxFetchBytes) {
                    throw new VendorError("Blob " + ref.objectId() + " is " + ref.size()
                            + " bytes, over maxFetchBytes (" + maxFetchBytes + ")");
                }
                byte[] data;
                try {
                    data = store.get(keyPrefix + ref.objectId(), ref.size() + 1);
                } catch (ObjectMissingException e) {
                    throw new VendorError("Blob " + ref.objectId() + " is gone", 404, "not_found");
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new VendorError("Blob " + ref.objectId() + " download failed ("
                            + e.getClass().getSimpleName() + ")");
                }
                if (data.length != ref.size()) {
                    throw new VendorError("Blob " + ref.objectId() + " size mismatch: expected " + ref.size()
                            + ", got " + data.length, null, VendorError.SIZE_MISMATCH);
                }
                String actual = sha256Hex(data);
                if (!actual.equalsIgnoreCase(ref.sha256())) {
                    throw new VendorError("Blob " + ref.objectId() + " SHA-256 mismatch", null,
                            VendorError.SHA256_MISMATCH);
                }
                return data;
            }

            @Override
            public void deleteRefs(List<BlobRef> refs) {
                if (deleter == null) return;
                for (BlobRef ref : refs) {
                    try {
                        deleter.accept(keyPrefix + ref.objectId());
                    } catch (RuntimeException ignored) {
                        // best effort
                    }
                }
            }
            @Override public void deleteScope(String shard, String executionId) { }

            @Override
            public void close() {
                if (closer == null) return;
                try {
                    closer.close();
                } catch (Exception ignored) {
                    // best effort
                }
            }
        };
    }

    private static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
