// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.buffering;

import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.storage.FunctionStorage;

/**
 * Process-wide handle to the worker's {@link FunctionStorage} backend, so a
 * buffering source producer can re-bind its {@link BoundStorage} after an HTTP
 * state-token round-trip. The HTTP transport is stateless: it serializes a
 * producer's state between {@code /init} and {@code /exchange}, and a live
 * storage view (a SQLite connection) can't be serialized. So buffering source
 * producers keep their {@code storage} view {@code transient} and re-acquire it
 * here from the {@code (executionId, attachId)} they did serialize.
 *
 * <p>A single worker process serves one storage backend, so a static handle is
 * sufficient; {@code VgiServiceImpl} registers it at construction.</p>
 */
public final class BufferingStorageHolder {

    private static volatile FunctionStorage backend;

    private BufferingStorageHolder() {}

    /**
     * Register the worker's storage backend. Called once when the service is built.
     *
     * @param b the worker's {@link FunctionStorage} backend
     */
    public static void register(FunctionStorage b) { backend = b; }

    /**
     * Re-bind a {@link BoundStorage} for {@code executionId} on the registered
     * backend (used when resuming a buffering source producer from a state token).
     *
     * @param executionId   the buffering execution id the storage is scoped to
     * @param attachId      the attach plaintext (for per-attach shard routing), or {@code null}
     * @return a storage view bound to {@code executionId}
     * @throws IllegalStateException if no backend has been registered
     */
    public static BoundStorage bind(byte[] executionId, byte[] attachId) {
        FunctionStorage b = backend;
        if (b == null) {
            throw new IllegalStateException("no buffering storage backend registered");
        }
        return new BoundStorage(b, executionId, attachId);
    }
}
