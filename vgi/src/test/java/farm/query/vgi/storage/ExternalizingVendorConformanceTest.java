// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import farm.query.vgirpc.vendor.VendorBlobClient;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link FunctionStorageConformanceTest} over {@link ExternalizingStorage} with
 * a threshold of one byte against vgi-rpc-python's fake blob vendor: every
 * non-empty value round-trips through the vendor.
 *
 * <p>Runs when {@code VGI_BLOB_VENDOR_URL} names a fake vendor (the CI unit-test
 * job starts one with {@code uvx}); skipped otherwise.</p>
 */
class ExternalizingVendorConformanceTest extends FunctionStorageConformanceTest {
    @Override
    FunctionStorage createStore() {
        String url = System.getenv("VGI_BLOB_VENDOR_URL");
        assumeTrue(url != null && !url.isBlank(), "VGI_BLOB_VENDOR_URL (a fake vendor) not set");
        String token = System.getenv().getOrDefault("VGI_BLOB_VENDOR_TOKEN", "test-token");
        return new ExternalizingStorage(new SqliteFunctionStorage(":memory:"),
                new VendorBlobClient(url, token), 1, null);
    }
}
