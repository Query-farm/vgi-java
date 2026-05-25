// Copyright 2025-2026 Query.Farm LLC

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
 */
public final class StorageResolver {

    private StorageResolver() {}

    public static final String ENV_VAR = "VGI_WORKER_SHARED_STORAGE";

    /** Resolve the configured backend. Throws on an unknown value. */
    public static FunctionStorage fromEnv() {
        String raw = System.getenv(ENV_VAR);
        String backend = raw == null ? "sqlite" : raw.trim().toLowerCase();
        switch (backend) {
            case "memory":
                return new SqliteFunctionStorage(":memory:");
            case "":
            case "sqlite":
                return new SqliteFunctionStorage(SqliteFunctionStorage.defaultDbPath());
            case "cloudflare-do":
                return CfdoStorage.fromEnv();
            default:
                throw new IllegalArgumentException(
                        ENV_VAR + "=" + backend + " unknown (supported: memory, sqlite, cloudflare-do)");
        }
    }
}
