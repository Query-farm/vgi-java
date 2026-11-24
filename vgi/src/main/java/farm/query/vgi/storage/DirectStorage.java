// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.storage;

import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.ObjectStore;
import farm.query.vgirpc.external.UploadUrlProvider;

import java.net.URI;
import java.util.function.Consumer;

/**
 * The worker's own bucket (direct S3 or GCS) behind the startup check: every role
 * (result externalization, upload grants, StateRef / argument objects) runs the bucket
 * check once before its first request, unless it already ran at startup
 * ({@code VGI_STORAGE_S3_VERIFY=1}). A failed check is not cached, so fixing the bucket
 * takes effect without a restart.
 */
public final class DirectStorage implements ExternalStorage, ObjectStore, UploadUrlProvider, AutoCloseable {

    private final String description;
    private final Object delegate;
    private final Runnable verify;
    private final boolean uploadGrants;
    private final Consumer<URI> urlValidator;
    private final AutoCloseable closer;
    private volatile boolean verified;

    /**
     * Wrap a store.
     *
     * @param description  a log-safe name, e.g. {@code "s3 bucket vgi at http://minio:9000"}
     * @param delegate     the store; an {@link ExternalStorage} and an {@link ObjectStore}, and an
     *                     {@link UploadUrlProvider} when {@code uploadGrants}
     * @param verify       the bucket check (throws an actionable {@link IllegalStateException})
     * @param uploadGrants whether the store issues upload grants
     * @param urlValidator accepts inbound pointer URLs on the store's signing host
     * @param closer       released by {@link #close()}
     */
    public DirectStorage(String description, Object delegate, Runnable verify, boolean uploadGrants,
                         Consumer<URI> urlValidator, AutoCloseable closer) {
        if (!(delegate instanceof ExternalStorage) || !(delegate instanceof ObjectStore)) {
            throw new IllegalArgumentException("direct storage must be an ExternalStorage and an ObjectStore");
        }
        if (uploadGrants && !(delegate instanceof UploadUrlProvider)) {
            throw new IllegalArgumentException("upload grants need an UploadUrlProvider");
        }
        this.description = description;
        this.delegate = delegate;
        this.verify = verify;
        this.uploadGrants = uploadGrants;
        this.urlValidator = urlValidator;
        this.closer = closer;
    }

    /** Run the bucket check now (startup), once. */
    public void verifyNow() {
        ensureVerified();
    }

    private void ensureVerified() {
        if (verified) return;
        synchronized (this) {
            if (verified) return;
            verify.run();
            verified = true;
        }
    }

    /** @return a log-safe description of the store */
    public String description() { return description; }

    /** @return whether the store issues upload grants (not with {@code VGI_STORAGE_S3_CHECKSUM=none}) */
    public boolean supportsUploadGrants() { return uploadGrants; }

    /** @return the inbound-pointer URL validator, pinned to the store's signing host */
    public Consumer<URI> urlValidator() { return urlValidator; }

    @Override
    public URI upload(byte[] body, String contentEncoding) throws Exception {
        ensureVerified();
        return ((ExternalStorage) delegate).upload(body, contentEncoding);
    }

    @Override
    public String putObject(byte[] data) throws Exception {
        ensureVerified();
        return ((ObjectStore) delegate).putObject(data);
    }

    @Override
    public byte[] get(String objectId, long maxBytes) throws Exception {
        ensureVerified();
        return ((ObjectStore) delegate).get(objectId, maxBytes);
    }

    @Override
    public UploadUrl generateUploadUrl(long size, String sha256Hex) throws Exception {
        if (!uploadGrants) throw new UnsupportedOperationException(description + " issues no upload grants");
        ensureVerified();
        return ((UploadUrlProvider) delegate).generateUploadUrl(size, sha256Hex);
    }

    @Override
    public void close() {
        try {
            closer.close();
        } catch (Exception ignored) {
            // best effort
        }
    }
}
