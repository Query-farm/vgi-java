// Copyright 2025-2026 Query.Farm LLC

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

    public BufferingStorage(FunctionStorage store, byte[] executionId) {
        this.store = store;
        this.executionId = executionId;
    }

    public long stateAppend(byte[] ns, byte[] key, byte[] value) {
        return store.stateAppend(executionId, ns, key, value);
    }

    public List<FunctionStorage.LogEntry> stateLogScan(byte[] ns, byte[] key, long afterId, int limit) {
        return store.stateLogScan(executionId, ns, key, afterId, limit);
    }

    public byte[] executionId() {
        return executionId;
    }
}
