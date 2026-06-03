// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.buffering;

import farm.query.vgi.storage.FunctionStorage;

import java.util.List;

/**
 * A {@link FunctionStorage} view bound to one {@code execution_id}. Handed to
 * a buffering function's {@code process}/{@code combine}/finalize so it can
 * stash and resume buffered batches. Mirrors vgi-python {@code params.storage}.
 *
 * <p>The backing store is whichever tier {@code VGI_WORKER_SHARED_STORAGE}
 * selected (in-process {@code :memory:}, local file, or a Cloudflare Durable
 * Object), so buffered state survives worker-process fan-out.
 */
public final class BufferingStorage {

    private final FunctionStorage store;
    private final byte[] executionId;

    /**
     * Binds a backing store to one {@code execution_id}.
     *
     * @param store the shared backing store (in-process, file, or Durable Object).
     * @param executionId the opaque {@code execution_id} all calls are scoped to.
     */
    public BufferingStorage(FunctionStorage store, byte[] executionId) {
        this.store = store;
        this.executionId = executionId;
    }

    /**
     * Appends a value to the append-log for {@code (executionId, ns, key)}.
     *
     * @param ns the namespace bytes.
     * @param key the key bytes within the namespace.
     * @param value the value bytes to append.
     * @return the monotonically increasing log id assigned to the appended entry.
     */
    public long stateAppend(byte[] ns, byte[] key, byte[] value) {
        return store.stateAppend(executionId, ns, key, value);
    }

    /**
     * Scans the append-log for {@code (executionId, ns, key)} after a given id.
     *
     * @param ns the namespace bytes.
     * @param key the key bytes within the namespace.
     * @param afterId return only entries with id strictly greater than this.
     * @param limit the maximum number of entries to return.
     * @return the matching log entries in id order.
     */
    public List<FunctionStorage.LogEntry> stateLogScan(byte[] ns, byte[] key, long afterId, int limit) {
        return store.stateLogScan(executionId, ns, key, afterId, limit);
    }

    /**
     * Returns the {@code execution_id} this view is scoped to.
     *
     * @return the opaque {@code execution_id} bytes.
     */
    public byte[] executionId() {
        return executionId;
    }
}
