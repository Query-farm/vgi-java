// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/**
 * Picks the {@link FunctionStorage} tier at worker startup from
 * {@code VGI_WORKER_SHARED_STORAGE}, mirroring vgi-python's
 * {@code vgi.function._resolve_storage} and vgi-go's {@code resolve.FromEnv}:
 *
 * <ul>
 *   <li>{@code memory}        — in-process {@link SqliteFunctionStorage} at
 *       {@code :memory:}. Process-local, no cross-process coordination;
 *       single-process deployments only.</li>
 *   <li>{@code sqlite} (default, or unset) — local cross-process
 *       {@link SqliteFunctionStorage} at a file (honors
 *       {@code VGI_WORKER_SQLITE_PATH}).</li>
 *   <li>{@code cloudflare-do} — distributed {@link CfdoStorage} (requires
 *       {@code VGI_CF_DO_URL}, optionally {@code VGI_CF_DO_TOKEN}).</li>
 * </ul>
 *
 * <p>The backend is then wrapped in {@link ExternalizingStorage} when the
 * environment asks for it ({@link ExternalizingStorage#wrapFromEnv}):
 * {@code cloudflare-do} always (its per-value cap is
 * {@value #CFDO_MAX_VALUE_BYTES} bytes), any other backend with
 * {@code VGI_STORAGE_EXTERNALIZE=1}, which keeps values of at least
 * {@code VGI_STORAGE_EXTERNALIZE_BYTES} in the blob vendor
 * ({@code VGI_BLOB_VENDOR_URL}).</p>
 */
public final class StorageResolver {

    private StorageResolver() {}

    /** The environment variable selecting the storage tier. */
    public static final String ENV_VAR = "VGI_WORKER_SHARED_STORAGE";

    /** The Cloudflare Durable Object's per-value cap, in bytes. */
    public static final long CFDO_MAX_VALUE_BYTES = 1024L * 1024;

    /**
     * Resolves the configured backend.
     *
     * @return the storage backend named by {@code VGI_WORKER_SHARED_STORAGE}
     *     ({@code sqlite} when unset), wrapped as the environment asks
     * @throws IllegalArgumentException if the value is not one of
     *     {@code memory}, {@code sqlite}, or {@code cloudflare-do}, or the
     *     externalization variables are inconsistent
     */
    public static FunctionStorage fromEnv() {
        return fromEnv(System.getenv());
    }

    /**
     * Resolves the configured backend from an explicit environment.
     *
     * @param env the environment mapping
     * @return the storage backend, wrapped as the environment asks
     * @throws IllegalArgumentException as for {@link #fromEnv()}
     */
    public static FunctionStorage fromEnv(java.util.Map<String, String> env) {
        String raw = env.get(ENV_VAR);
        String backend = raw == null ? "sqlite" : raw.trim().toLowerCase();
        switch (backend) {
            case "memory":
                return ExternalizingStorage.wrapFromEnv(new SqliteFunctionStorage(":memory:"), null, env);
            case "":
            case "sqlite":
                return ExternalizingStorage.wrapFromEnv(
                        new SqliteFunctionStorage(SqliteFunctionStorage.defaultDbPath()), null, env);
            case "cloudflare-do":
                return ExternalizingStorage.wrapFromEnv(CfdoStorage.fromEnv(), CFDO_MAX_VALUE_BYTES, env);
            default:
                throw new IllegalArgumentException(
                        ENV_VAR + "=" + backend + " unknown (supported: memory, sqlite, cloudflare-do)");
        }
    }
}
