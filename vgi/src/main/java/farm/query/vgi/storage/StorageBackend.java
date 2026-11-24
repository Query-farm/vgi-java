// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.storage;

import farm.query.vgirpc.vendor.BlobVendor;
import farm.query.vgirpc.vendor.BlobVendorHttpSettings;
import farm.query.vgirpc.vendor.VendorBlobClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Selects where large values go (direct object storage contract v1, the same in every SDK).
 *
 * <p>{@value #ENV_BACKEND} = {@code vendor} | {@code s3} | {@code gcs} | {@code none}. Unset:
 * {@code vendor} if {@value #ENV_VENDOR_URL} is set, else {@code s3} if {@value #ENV_S3_BUCKET}
 * is set, else {@code gcs} if {@value #ENV_GCS_BUCKET} is set, else {@code none} (everything
 * inline). More than one of those three set without {@value #ENV_BACKEND} refuses to start.</p>
 *
 * <p>{@code s3} and {@code gcs} use the worker's own bucket for every large-value path:
 * externalized results, upload grants and argument references (S3 with
 * {@code VGI_STORAGE_S3_CHECKSUM=sha256} only), StateRef objects, and function storage. They
 * need {@code farm.query:vgirpc-s3} / {@code farm.query:vgirpc-gcs} on the classpath. The bucket
 * is checked (a HEAD) on first use, or at startup with {@code VGI_STORAGE_S3_VERIFY=1}.</p>
 */
public final class StorageBackend {

    /** The backend selector. */
    public static final String ENV_BACKEND = "VGI_STORAGE_BACKEND";
    /** The blob vendor's URL. */
    public static final String ENV_VENDOR_URL = "VGI_BLOB_VENDOR_URL";
    /** The direct S3 bucket. */
    public static final String ENV_S3_BUCKET = "VGI_STORAGE_S3_BUCKET";
    /** The direct GCS bucket. */
    public static final String ENV_GCS_BUCKET = "VGI_STORAGE_GCS_BUCKET";
    /** {@code 1} = check the bucket at startup. */
    public static final String ENV_S3_VERIFY = "VGI_STORAGE_S3_VERIFY";
    /** Orchard hosted mode. */
    static final String ENV_HOSTED = "VGI_ORCHARD_HOSTED";

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(StorageBackend.class);
    private static final Set<String> TRUE = Set.of("1", "true", "yes", "on");

    /** Where large values go. */
    public enum Kind {
        /** The blob vendor ({@code VGI_BLOB_VENDOR_URL}). */
        VENDOR,
        /** The worker's own S3 / S3-compatible bucket. */
        S3,
        /** The worker's own GCS bucket. */
        GCS,
        /** Nowhere: everything stays inline. */
        NONE
    }

    private StorageBackend() {}

    private static boolean set(Map<String, String> env, String name) {
        String v = env.get(name);
        return v != null && !v.isBlank();
    }

    /**
     * The backend the environment selects.
     *
     * @param env environment mapping
     * @return the backend
     * @throws IllegalArgumentException for an unknown {@value #ENV_BACKEND}, or more than one
     *     backend configured without it
     */
    public static Kind select(Map<String, String> env) {
        String raw = env.getOrDefault(ENV_BACKEND, "").strip().toLowerCase(Locale.ROOT);
        if (!raw.isEmpty()) {
            return switch (raw) {
                case "vendor" -> Kind.VENDOR;
                case "s3" -> Kind.S3;
                case "gcs" -> Kind.GCS;
                case "none" -> Kind.NONE;
                default -> throw new IllegalArgumentException(ENV_BACKEND + " must be vendor, s3, gcs or none, got '"
                        + raw + "'");
            };
        }
        List<String> configured = new ArrayList<>();
        if (set(env, ENV_VENDOR_URL)) configured.add(ENV_VENDOR_URL);
        if (set(env, ENV_S3_BUCKET)) configured.add(ENV_S3_BUCKET);
        if (set(env, ENV_GCS_BUCKET)) configured.add(ENV_GCS_BUCKET);
        if (configured.size() > 1) {
            throw new IllegalArgumentException("more than one storage backend is configured ("
                    + String.join(", ", configured) + "): set " + ENV_BACKEND + " to vendor, s3, gcs or none");
        }
        if (configured.isEmpty()) return Kind.NONE;
        return switch (configured.get(0)) {
            case ENV_VENDOR_URL -> Kind.VENDOR;
            case ENV_S3_BUCKET -> Kind.S3;
            default -> Kind.GCS;
        };
    }

    /**
     * Open the direct store the environment selects ({@code s3} or {@code gcs}).
     *
     * @param kind {@link Kind#S3} or {@link Kind#GCS}
     * @param env  environment mapping
     * @return the store behind its startup check
     * @throws IllegalStateException if the backend's module is not on the classpath
     * @throws IllegalArgumentException if a variable is missing or malformed
     */
    public static DirectStorage openDirect(Kind kind, Map<String, String> env) {
        try {
            return switch (kind) {
                case S3 -> S3Backend.open(env);
                case GCS -> GcsBackend.open(env);
                default -> throw new IllegalArgumentException(kind + " is not a direct storage backend");
            };
        } catch (NoClassDefFoundError e) {
            String module = kind == Kind.S3 ? "farm.query:vgirpc-s3" : "farm.query:vgirpc-gcs";
            throw new IllegalStateException(ENV_BACKEND + "=" + kind.name().toLowerCase(Locale.ROOT)
                    + " needs " + module + " on the worker's classpath");
        }
    }

    /**
     * The HTTP server's storage settings from the environment: the vendor, the worker's own
     * bucket (results, upload grants unless checksum none, StateRef and argument objects), or
     * nothing beyond {@code VGI_RPC_MAX_REQUEST_BYTES}.
     *
     * @param env environment mapping
     * @return the settings
     * @throws IllegalArgumentException if the configuration is ambiguous or malformed
     * @throws IllegalStateException if the bucket check fails at startup ({@value #ENV_S3_VERIFY}=1)
     */
    public static BlobVendorHttpSettings httpSettingsFromEnv(Map<String, String> env) {
        Kind kind = select(env);
        if (kind == Kind.VENDOR && !set(env, ENV_VENDOR_URL)) {
            throw new IllegalArgumentException(ENV_BACKEND + "=vendor needs " + ENV_VENDOR_URL);
        }
        return switch (kind) {
            case VENDOR -> BlobVendorHttpSettings.fromEnv(env);
            case NONE -> new BlobVendorHttpSettings(null, null, null, BlobVendor.maxRequestBytesFromEnv(env));
            case S3, GCS -> {
                DirectStorage store = openDirect(kind, env);
                if (TRUE.contains(env.getOrDefault(ENV_S3_VERIFY, "").strip().toLowerCase(Locale.ROOT))) {
                    store.verifyNow();
                }
                if (TRUE.contains(env.getOrDefault(ENV_HOSTED, "").strip().toLowerCase(Locale.ROOT))) {
                    LOG.warn("Orchard hosted mode with {} storage instead of the blob vendor (operator's choice)",
                            kind.name().toLowerCase(Locale.ROOT));
                }
                if (!store.supportsUploadGrants()) {
                    LOG.info("{}: no upload grants, so argument references and large inputs stay inline "
                            + "(VGI-Upload-URL-Support and VGI-Arguments-Ref-Bytes are not advertised); results "
                            + "and StateRef objects are still externalized", store.description());
                }
                yield BlobVendorHttpSettings.forStorage(store, store.supportsUploadGrants() ? store : null,
                        store.urlValidator(), env);
            }
        };
    }

    /**
     * The blob store for function storage ({@link ExternalizingStorage}) from the environment.
     *
     * @param env           environment mapping
     * @param maxFetchBytes the largest blob read back
     * @return the store, or {@code null} with no backend
     */
    public static BlobStore blobStoreFromEnv(Map<String, String> env, long maxFetchBytes) {
        Kind kind = select(env);
        if (kind == Kind.VENDOR && !set(env, ENV_VENDOR_URL)) return null;
        return switch (kind) {
            case VENDOR -> BlobStore.of(VendorBlobClient.fromEnv("fs", env));
            case NONE -> null;
            case S3, GCS -> {
                // Function storage keeps its values under <prefix>fs/, apart from results and
                // StateRef objects (the vendor separates them by purpose).
                String prefixVar = kind == Kind.S3 ? "VGI_STORAGE_S3_PREFIX" : "VGI_STORAGE_GCS_PREFIX";
                Map<String, String> fsEnv = new java.util.HashMap<>(env);
                String prefix = env.getOrDefault(prefixVar, "").strip();
                String fsPrefix = (prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/") + "fs/";
                fsEnv.put(prefixVar, fsPrefix);
                DirectStorage store = openDirect(kind, fsEnv);
                yield BlobStore.of(store, fsPrefix, maxFetchBytes, store);
            }
        };
    }
}
