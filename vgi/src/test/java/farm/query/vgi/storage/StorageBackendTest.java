// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.storage;

import farm.query.vgirpc.vendor.BlobVendorHttpSettings;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Backend selection and env wiring (direct object storage contract v1, §1-§2). No network. */
class StorageBackendTest {

    private static Map<String, String> env(String... kv) {
        Map<String, String> env = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) env.put(kv[i], kv[i + 1]);
        return env;
    }

    private static final String[] S3 = {
        "VGI_STORAGE_S3_BUCKET", "b",
        "VGI_STORAGE_S3_ENDPOINT", "http://127.0.0.1:9",
        "VGI_STORAGE_S3_ACCESS_KEY_ID", "k",
        "VGI_STORAGE_S3_SECRET_ACCESS_KEY", "s",
        "VGI_STORAGE_ALLOW_PLAIN_HTTP", "1",
    };

    @Test
    void selection() {
        assertEquals(StorageBackend.Kind.NONE, StorageBackend.select(env()));
        assertEquals(StorageBackend.Kind.VENDOR, StorageBackend.select(env("VGI_BLOB_VENDOR_URL", "https://v")));
        assertEquals(StorageBackend.Kind.S3, StorageBackend.select(env("VGI_STORAGE_S3_BUCKET", "b")));
        assertEquals(StorageBackend.Kind.GCS, StorageBackend.select(env("VGI_STORAGE_GCS_BUCKET", "b")));
        IllegalArgumentException conflict = assertThrows(IllegalArgumentException.class, () -> StorageBackend.select(
                env("VGI_BLOB_VENDOR_URL", "https://v", "VGI_STORAGE_S3_BUCKET", "b")));
        assertTrue(conflict.getMessage().contains("VGI_BLOB_VENDOR_URL"), conflict.getMessage());
        assertTrue(conflict.getMessage().contains("VGI_STORAGE_S3_BUCKET"), conflict.getMessage());
        assertEquals(StorageBackend.Kind.S3, StorageBackend.select(
                env("VGI_BLOB_VENDOR_URL", "https://v", "VGI_STORAGE_S3_BUCKET", "b", "VGI_STORAGE_BACKEND", "s3")));
        assertEquals(StorageBackend.Kind.NONE, StorageBackend.select(
                env("VGI_STORAGE_S3_BUCKET", "b", "VGI_STORAGE_BACKEND", "none")));
        assertThrows(IllegalArgumentException.class, () -> StorageBackend.select(env("VGI_STORAGE_BACKEND", "r2")));
    }

    @Test
    void noneKeepsOnlyTheRequestCap() {
        BlobVendorHttpSettings s = StorageBackend.httpSettingsFromEnv(env("VGI_RPC_MAX_REQUEST_BYTES", "32768"));
        assertNull(s.externalLocation());
        assertNull(s.uploadUrlProvider());
        assertEquals(32768L, s.maxRequestBytes());
        assertThrows(IllegalArgumentException.class,
                () -> StorageBackend.httpSettingsFromEnv(env("VGI_STORAGE_BACKEND", "vendor")));
    }

    @Test
    void s3WiresEveryRole() {
        Map<String, String> e = env(S3);
        e.put("VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", "4096");
        e.put("VGI_RPC_MAX_UPLOAD_BYTES", "1048576");
        BlobVendorHttpSettings s = StorageBackend.httpSettingsFromEnv(e);
        assertNotNull(s.externalLocation());
        assertInstanceOf(DirectStorage.class, s.externalLocation().storage());
        assertEquals(4096, s.externalLocation().thresholdBytes());
        assertNotNull(s.uploadUrlProvider(), "sha256 issues upload grants");
        assertEquals(1048576L, s.maxUploadBytes());
        // The inbound validator pins the signing host: the configured endpoint, http allowed.
        s.externalLocation().urlValidator().accept(java.net.URI.create("http://127.0.0.1:9/b/rpc/x"));
        assertThrows(IllegalArgumentException.class,
                () -> s.externalLocation().urlValidator().accept(java.net.URI.create("http://127.0.0.1:9/b/fs/x")));
        assertThrows(IllegalArgumentException.class,
                () -> s.externalLocation().urlValidator().accept(java.net.URI.create("http://evil.example/b/x")));
        ((DirectStorage) s.externalLocation().storage()).close();
    }

    @Test
    void checksumNoneIssuesNoUploadGrants() {
        Map<String, String> e = env(S3);
        e.put("VGI_STORAGE_S3_CHECKSUM", "none");
        e.put("VGI_RPC_MAX_UPLOAD_BYTES", "1048576");
        BlobVendorHttpSettings s = StorageBackend.httpSettingsFromEnv(e);
        assertNotNull(s.externalLocation(), "results are still externalized");
        assertNull(s.uploadUrlProvider(), "no upload grants, so no upload URLs or argument references");
        assertNull(s.maxUploadBytes());
        DirectStorage d = (DirectStorage) s.externalLocation().storage();
        assertThrows(UnsupportedOperationException.class, () -> d.generateUploadUrl(1, "0".repeat(64)));
        d.close();
    }

    @Test
    void verifyAtStartupFailsWithAnActionableMessage() {
        Map<String, String> e = env(S3);
        e.put("VGI_STORAGE_S3_VERIFY", "1");
        IllegalStateException err = assertThrows(IllegalStateException.class,
                () -> StorageBackend.httpSettingsFromEnv(e));
        assertTrue(err.getMessage().contains("unreachable"), err.getMessage());
    }

    @Test
    void plainHttpNeedsTheOptIn() {
        Map<String, String> e = env(S3);
        e.remove("VGI_STORAGE_ALLOW_PLAIN_HTTP");
        IllegalArgumentException err = assertThrows(IllegalArgumentException.class,
                () -> StorageBackend.httpSettingsFromEnv(e));
        assertTrue(err.getMessage().contains("VGI_STORAGE_ALLOW_PLAIN_HTTP"), err.getMessage());
    }

    @Test
    void functionStorageUsesTheSameSelection() {
        assertNull(StorageBackend.blobStoreFromEnv(env(), 1 << 20));
        BlobStore store = StorageBackend.blobStoreFromEnv(env(S3), 1 << 20);
        assertNotNull(store);
        store.close();
    }
}
