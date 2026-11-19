// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import farm.query.vgirpc.vendor.BlobRef;
import farm.query.vgirpc.vendor.VendorBlobClient;
import farm.query.vgirpc.vendor.VendorError;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** The externalizing wrapper: envelope on the backend, env wiring, error contract, and the vendor path. */
class ExternalizingStorageTest {

    private static final byte[] SCOPE = {1, 2, 3, 4};
    private static final byte[] NS = "ns".getBytes(StandardCharsets.UTF_8);
    // The SDK-neutral error patterns from docs/testing/blob-test-catalog.md (RE2, '.' matches newline).
    private static final Pattern INTEGRITY = Pattern.compile(".*storage blob.*(sha256|size) mismatch.*", Pattern.DOTALL);
    private static final Pattern MISSING = Pattern.compile(".*storage blob.*(missing|not found).*", Pattern.DOTALL);

    private static byte[] bytes(int n, char c) {
        byte[] b = new byte[n];
        Arrays.fill(b, (byte) c);
        return b;
    }

    // --- no vendor ---------------------------------------------------------

    @Test
    void backendHoldsInlineEnvelopes() {
        SqliteFunctionStorage base = new SqliteFunctionStorage(":memory:");
        try (ExternalizingStorage s = new ExternalizingStorage(base, null, 4, null)) {
            s.statePut(SCOPE, NS, new byte[] {9}, new byte[] {1, 2, 3, 4, 5, 6});
            assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 6}, base.stateGet(SCOPE, NS, new byte[] {9}));
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6}, s.stateGet(SCOPE, NS, new byte[] {9}));
            // Counters are not enveloped.
            s.stateCounterAdd(SCOPE, NS, new byte[] {7}, 5);
            assertEquals(5, base.stateCounterGet(SCOPE, NS, new byte[] {7}));
        }
    }

    @Test
    void rawValuesAndPointersWithoutAVendorAreRefused() {
        SqliteFunctionStorage base = new SqliteFunctionStorage(":memory:");
        try (ExternalizingStorage s = new ExternalizingStorage(base, null, 4, null)) {
            base.statePut(SCOPE, NS, new byte[] {1}, new byte[] {2, 'r', 'a', 'w'});
            assertThrows(StorageEnvelopeException.class, () -> s.stateGet(SCOPE, NS, new byte[] {1}));
            base.statePut(SCOPE, NS, new byte[] {2}, StorageEnvelope.encodePointer(
                    new BlobRef("0123456789abcdef0123456789abcdef", 1, "00".repeat(32))));
            StorageEnvelopeException e = assertThrows(StorageEnvelopeException.class,
                    () -> s.stateGet(SCOPE, NS, new byte[] {2}));
            assertTrue(e.getMessage().contains("VGI_BLOB_VENDOR_URL"), e.getMessage());
        }
    }

    @Test
    void cappedBackendRefusesOversizedValuesWithoutAVendor() {
        try (ExternalizingStorage s = new ExternalizingStorage(new SqliteFunctionStorage(":memory:"), null, 8, 8L)) {
            s.statePut(SCOPE, NS, new byte[] {1}, bytes(7, 'a'));  // envelope = 8 bytes, fits
            StorageValueTooLargeException e = assertThrows(StorageValueTooLargeException.class,
                    () -> s.statePut(SCOPE, NS, new byte[] {2}, bytes(8, 'a')));
            assertTrue(e.getMessage().contains("VGI_BLOB_VENDOR_URL"), e.getMessage());
        }
        assertThrows(IllegalArgumentException.class,
                () -> new ExternalizingStorage(new SqliteFunctionStorage(":memory:"), null, 9, 8L));
        assertThrows(IllegalArgumentException.class,
                () -> new ExternalizingStorage(new SqliteFunctionStorage(":memory:"), null, 0, null));
    }

    @Test
    void wrapFromEnv() throws Exception {
        FunctionStorage base = new SqliteFunctionStorage(":memory:");
        assertSame(base, ExternalizingStorage.wrapFromEnv(base, null, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> ExternalizingStorage.wrapFromEnv(base, null, Map.of("VGI_STORAGE_EXTERNALIZE", "1")));
        // A capped backend (the Durable Object) is always wrapped, vendor or not.
        FunctionStorage capped = ExternalizingStorage.wrapFromEnv(base, 1024L * 1024, Map.of());
        assertInstanceOf(ExternalizingStorage.class, capped);
        assertEquals(512L * 1024, ((ExternalizingStorage) capped).threshold());
        assertNull(((ExternalizingStorage) capped).blobClient());
        FunctionStorage wrapped = ExternalizingStorage.wrapFromEnv(base, null, Map.of(
                "VGI_STORAGE_EXTERNALIZE", "true",
                "VGI_BLOB_VENDOR_URL", "https://blobs.example.com",
                "VGI_BLOB_VENDOR_TOKEN", "t",
                "VGI_CF_DO_EXTERNALIZE_BYTES", "4096"));
        assertEquals(4096, ((ExternalizingStorage) wrapped).threshold());
        wrapped.close();
        assertThrows(IllegalArgumentException.class, () -> ExternalizingStorage.externalizeBytesFromEnv(
                Map.of("VGI_STORAGE_EXTERNALIZE_BYTES", "-1")));
        FunctionStorage resolved = StorageResolver.fromEnv(Map.of("VGI_WORKER_SHARED_STORAGE", "memory"));
        assertInstanceOf(SqliteFunctionStorage.class, resolved);
        resolved.close();
    }

    @Test
    void errorsFollowTheSdkNeutralContract() {
        BlobRef ref = new BlobRef("abc123", 10, "00".repeat(32));
        String sha = ExternalizingStorage.blobReadError(ref, new VendorError("x", null, VendorError.SHA256_MISMATCH))
                .getMessage();
        assertEquals("storage blob abc123 failed its integrity check: sha256 mismatch", sha);
        assertTrue(INTEGRITY.matcher(sha).matches());
        StorageBlobException size = ExternalizingStorage.blobReadError(ref,
                new VendorError("x", null, VendorError.SIZE_MISMATCH));
        assertInstanceOf(StorageBlobIntegrityException.class, size);
        assertTrue(INTEGRITY.matcher(size.getMessage()).matches());
        for (VendorError gone : List.of(new VendorError("x", 404, "NoSuchKey"), new VendorError("x", 404, "not_found"),
                new VendorError("x", null, "not_uploaded"))) {
            StorageBlobException m = ExternalizingStorage.blobReadError(ref, gone);
            assertInstanceOf(StorageBlobMissingException.class, m);
            assertTrue(MISSING.matcher(m.getMessage()).matches(), m.getMessage());
        }
        StorageBlobException other = ExternalizingStorage.blobReadError(ref, new VendorError("boom", 500, null));
        assertTrue(other.getMessage().startsWith("storage blob abc123 could not be read: "), other.getMessage());
    }

    // --- against the fake vendor ---------------------------------------------

    private static String vendorUrl() {
        String url = System.getenv("VGI_BLOB_VENDOR_URL");
        assumeTrue(url != null && !url.isBlank(), "VGI_BLOB_VENDOR_URL (a fake vendor with --test-routes) not set");
        return url;
    }

    private static VendorBlobClient client(String url) {
        return new VendorBlobClient(url, System.getenv().getOrDefault("VGI_BLOB_VENDOR_TOKEN", "test-token"));
    }

    private static JsonObject admin(String url, String path) throws Exception {
        HttpResponse<String> r = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(url + path)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode(), path);
        return JsonParser.parseString(r.body()).getAsJsonObject();
    }

    @Test
    void thresholdBoundaryCountsBytes() throws Exception {
        String url = vendorUrl();
        admin(url, "/__admin__/reset");
        int t = 4096;
        SqliteFunctionStorage base = new SqliteFunctionStorage(":memory:");
        try (ExternalizingStorage s = new ExternalizingStorage(base, client(url), t, null)) {
            FunctionStorage shard = s.forShard("att-0123");
            JsonObject before = admin(url, "/__stats__");
            List<FunctionStorage.KV> items = new ArrayList<>();
            for (int d = -1; d <= 1; d++) items.add(new FunctionStorage.KV(new byte[] {(byte) (d + 1)}, bytes(t + d, 'x')));
            shard.statePutMany(SCOPE, NS, items);
            JsonObject mid = admin(url, "/__stats__");
            assertEquals(2, mid.get("puts").getAsLong() - before.get("puts").getAsLong());
            assertEquals(2L * t + 1, mid.get("bytes_uploaded").getAsLong() - before.get("bytes_uploaded").getAsLong());
            // One below is inline, at and above are pointers.
            assertEquals(StorageEnvelope.TAG_INLINE, base.stateGet(SCOPE, NS, new byte[] {0})[0]);
            byte[] at = base.stateGet(SCOPE, NS, new byte[] {1});
            assertEquals(StorageEnvelope.TAG_POINTER, at[0]);
            assertEquals(t, StorageEnvelope.decodePointer(at).size());
            List<byte[]> back = shard.stateGetMany(SCOPE, NS, List.of(new byte[] {0}, new byte[] {1}, new byte[] {2}));
            for (int i = 0; i < 3; i++) assertArrayEquals(items.get(i).value(), back.get(i));
            JsonObject after = admin(url, "/__stats__");
            assertEquals(2, after.get("gets").getAsLong() - mid.get("gets").getAsLong());
            // executionClear deletes the scope's blobs (in the background).
            long objects = after.get("objects").getAsLong();
            shard.executionClear(SCOPE);
            s.flushBlobCleanup();
            assertEquals(objects - 2, admin(url, "/__stats__").get("objects").getAsLong());
        }
    }

    @Test
    void corruptAndMissingBlobsFailWithTheContractMessages() throws Exception {
        String url = vendorUrl();
        admin(url, "/__admin__/reset");
        try (ExternalizingStorage s = new ExternalizingStorage(new SqliteFunctionStorage(":memory:"), client(url), 16,
                null)) {
            s.statePut(SCOPE, NS, new byte[] {1}, bytes(64, 'c'));
            admin(url, "/__admin__/fault?purpose=fs&action=corrupt&count=1");
            StorageBlobException corrupt = assertThrows(StorageBlobException.class,
                    () -> s.stateGet(SCOPE, NS, new byte[] {1}));
            assertTrue(INTEGRITY.matcher(corrupt.getMessage()).matches(), corrupt.getMessage());
            admin(url, "/__admin__/reset");
            assertArrayEquals(bytes(64, 'c'), s.stateGet(SCOPE, NS, new byte[] {1}));

            s.stateAppend(SCOPE, NS, new byte[0], bytes(64, 'm'));
            admin(url, "/__admin__/fault?purpose=fs&action=missing&count=1");
            StorageBlobException missing = assertThrows(StorageBlobException.class,
                    () -> s.stateLogScan(SCOPE, NS, new byte[0], -1, 0));
            assertTrue(MISSING.matcher(missing.getMessage()).matches(), missing.getMessage());
            admin(url, "/__admin__/reset");
        }
    }

    @Test
    void queuePopAndDrainDeleteTheirBlobs() throws Exception {
        String url = vendorUrl();
        try (ExternalizingStorage s = new ExternalizingStorage(new SqliteFunctionStorage(":memory:"), client(url), 16,
                null)) {
            long deletes = admin(url, "/__stats__").get("deletes").getAsLong();
            s.queuePush(SCOPE, List.of(bytes(32, 'q'), new byte[] {1}));
            assertArrayEquals(bytes(32, 'q'), s.queuePop(SCOPE));
            assertArrayEquals(new byte[] {1}, s.queuePop(SCOPE));
            assertNull(s.queuePop(SCOPE));
            s.statePut(SCOPE, NS, new byte[] {5}, bytes(40, 'd'));
            List<FunctionStorage.KV> drained = s.stateDrain(SCOPE, NS);
            assertEquals(1, drained.size());
            assertArrayEquals(bytes(40, 'd'), drained.get(0).value());
            s.flushBlobCleanup();
            assertEquals(deletes + 2, admin(url, "/__stats__").get("deletes").getAsLong());
        }
    }
}
