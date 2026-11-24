// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.storage;

import farm.query.vgirpc.external.gcs.GcsStorage;

import java.util.Map;

/** Opens the direct GCS backend. Loaded only for {@code VGI_STORAGE_BACKEND=gcs}, so
 *  {@code farm.query:vgirpc-gcs} is needed on the classpath only then. Upload grants are
 *  issued (direct storage contract Addendum C): GCS pins the exact size through the signed
 *  {@code x-goog-content-length-range}; it cannot enforce a SHA-256, so the worker verifies the
 *  digest when it resolves the upload and refuses a mismatch. */
final class GcsBackend {
    private GcsBackend() {}

    static DirectStorage open(Map<String, String> env, String area) {
        GcsStorage gcs = GcsStorage.fromEnv(env, area);
        String endpoint = env.getOrDefault(GcsStorage.ENV_ENDPOINT, "").strip();
        String description = "gcs bucket " + env.get(GcsStorage.ENV_BUCKET).strip()
                + (endpoint.isEmpty() ? "" : " at " + farm.query.vgirpc.external.LocationResolver.redactUrl(endpoint));
        return new DirectStorage(description, gcs, gcs::verify, true, gcs.urlValidator(), gcs, gcs::deleteObject);
    }
}
