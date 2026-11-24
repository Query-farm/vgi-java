// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link FunctionStorageConformanceTest} over {@link ExternalizingStorage} with a threshold of
 * one byte on the direct S3 backend: every non-empty value round-trips through the worker's own
 * bucket, with the same selection the worker makes ({@link StorageBackend#blobStoreFromEnv}).
 *
 * <p>Runs when {@code VGI_TEST_S3_ENDPOINT} names an S3-compatible store (the CI S3 job runs
 * RustFS) with {@code VGI_TEST_S3_BUCKET}, {@code VGI_TEST_S3_ACCESS_KEY} and
 * {@code VGI_TEST_S3_SECRET_KEY}; skipped otherwise (the job fails on that skip).</p>
 */
class ExternalizingS3ConformanceTest extends FunctionStorageConformanceTest {
    @Override
    FunctionStorage createStore() {
        String endpoint = System.getenv("VGI_TEST_S3_ENDPOINT");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "VGI_TEST_S3_ENDPOINT (an S3-compatible store) not set");
        Map<String, String> env = new HashMap<>();
        env.put("VGI_STORAGE_BACKEND", "s3");
        env.put("VGI_STORAGE_S3_ENDPOINT", endpoint);
        env.put("VGI_STORAGE_S3_BUCKET", System.getenv("VGI_TEST_S3_BUCKET"));
        env.put("VGI_STORAGE_S3_ACCESS_KEY_ID", System.getenv("VGI_TEST_S3_ACCESS_KEY"));
        env.put("VGI_STORAGE_S3_SECRET_ACCESS_KEY", System.getenv("VGI_TEST_S3_SECRET_KEY"));
        env.put("VGI_STORAGE_S3_PREFIX", "conformance/");
        return ExternalizingStorage.withBlobStore(new SqliteFunctionStorage(":memory:"),
                StorageBackend.blobStoreFromEnv(env, 1L << 30), 1, null);
    }
}
