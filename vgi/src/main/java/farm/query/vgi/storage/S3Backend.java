// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.storage;

import farm.query.vgirpc.external.s3.S3Storage;

import java.util.Map;

/** Opens the direct S3 backend. Loaded only for {@code VGI_STORAGE_BACKEND=s3}, so
 *  {@code farm.query:vgirpc-s3} is needed on the classpath only then. */
final class S3Backend {
    private S3Backend() {}

    static DirectStorage open(Map<String, String> env) {
        S3Storage s3 = S3Storage.fromEnv(env);
        String endpoint = env.getOrDefault(S3Storage.ENV_ENDPOINT, "").strip();
        String description = "s3 bucket " + env.get(S3Storage.ENV_BUCKET).strip()
                + (endpoint.isEmpty() ? " (AWS)" : " at " + farm.query.vgirpc.external.LocationResolver.redactUrl(endpoint))
                + (s3.supportsUploadGrants() ? "" : ", checksum none");
        return new DirectStorage(description, s3, s3::verify, s3.supportsUploadGrants(), s3.urlValidator(), s3);
    }
}
