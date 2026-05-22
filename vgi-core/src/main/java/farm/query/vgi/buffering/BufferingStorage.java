// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.buffering;

import java.util.List;

/**
 * A {@link BufferingStore} view bound to one {@code execution_id}. Handed to
 * a buffering function's {@code process}/{@code combine}/finalize so it can
 * stash and resume buffered batches. Mirrors vgi-python {@code params.storage}.
 */
public final class BufferingStorage {

    private final BufferingStore store;
    private final byte[] executionId;

    public BufferingStorage(BufferingStore store, byte[] executionId) {
        this.store = store;
        this.executionId = executionId;
    }

    public long stateAppend(byte[] ns, byte[] key, byte[] value) {
        return store.append(executionId, ns, key, value);
    }

    public List<BufferingStore.Entry> stateLogScan(byte[] ns, byte[] key, long afterId, int limit) {
        return store.scan(executionId, ns, key, afterId, limit);
    }

    public byte[] executionId() {
        return executionId;
    }
}
