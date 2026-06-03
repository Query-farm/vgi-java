// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import java.util.List;

/**
 * The worker's shared-state surface, unified across the three deployment tiers
 * (mirrors vgi-python / vgi-go / vgi-typescript and the Cloudflare DO protocol):
 *
 * <ol>
 *   <li><b>in-process</b> — {@link SqliteFunctionStorage} at {@code :memory:};
 *       process-local, no cross-process coordination. Single-process only.</li>
 *   <li><b>local cross-process</b> — {@link SqliteFunctionStorage} at a file;
 *       WAL coordinates worker subprocesses / multi-worker HTTP on one box.</li>
 *   <li><b>distributed</b> — {@link CfdoStorage}, a Cloudflare Durable Object
 *       over HTTP, for multi-replica / edge deployments.</li>
 * </ol>
 *
 * <p>Selected at startup by {@link StorageResolver#fromEnv()} via
 * {@code VGI_WORKER_SHARED_STORAGE}. Every state row is addressed by
 * {@code (scope_id, ns, key)}, where {@code scope_id} is the execution_id
 * (buffering / aggregate) or transaction_opaque_data (transactions), and
 * {@code ns} namespaces the kind of state. Java workers have no work-queue
 * functions, so the queue surface of the other SDKs is intentionally absent.
 */
public interface FunctionStorage extends AutoCloseable {

    /**
     * One appended record from a state log: its monotonic cursor id and value.
     *
     * @param id    the append log's resumable cursor id (strictly increasing)
     * @param value the stored bytes
     */
    record LogEntry(long id, byte[] value) {}

    /**
     * A key/value pair for a batch put.
     *
     * @param key   the row key bytes
     * @param value the stored bytes
     */
    record KV(byte[] key, byte[] value) {}

    // --- append-only log (table-buffering) ---

    /**
     * Appends {@code value} to the {@code (scope, ns, key)} log.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param key     the log key bytes
     * @param value   the bytes to append
     * @return the resumable cursor id of the appended entry
     */
    long stateAppend(byte[] scopeId, byte[] ns, byte[] key, byte[] value);

    /**
     * Scans up to {@code limit} entries with id &gt; {@code afterId}, in order.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param key     the log key bytes
     * @param afterId scan entries strictly after this cursor id (0 = from start)
     * @param limit   maximum entries to return; {@code <= 0} means unbounded
     * @return the matching entries in ascending cursor-id order
     */
    List<LogEntry> stateLogScan(byte[] scopeId, byte[] ns, byte[] key, long afterId, int limit);

    // --- key/value (transactions, aggregate state) ---

    /**
     * Batch get over a namespace.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param keys    the keys to look up
     * @return one element per requested key, {@code null} where absent, in the
     *     same order as {@code keys}
     */
    List<byte[]> stateGetMany(byte[] scopeId, byte[] ns, List<byte[]> keys);

    /**
     * Batch upsert over a namespace.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param items   the key/value pairs to insert or replace
     */
    void statePutMany(byte[] scopeId, byte[] ns, List<KV> items);

    /**
     * Deletes the given keys (missing keys are ignored).
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param keys    the keys to delete
     */
    void stateDelete(byte[] scopeId, byte[] ns, List<byte[]> keys);

    // --- lifecycle ---

    /**
     * Wipes all state + log rows for {@code scopeId} across every namespace.
     *
     * @param scopeId the scope id whose state to clear
     * @return the number of rows removed
     */
    int executionClear(byte[] scopeId);

    /** Releases backend resources; the default is a no-op. */
    @Override
    default void close() {}

    // --- single-key conveniences ---

    /**
     * Single-key convenience over {@link #stateGetMany}.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param key     the key to look up
     * @return the stored bytes, or {@code null} if absent
     */
    default byte[] stateGet(byte[] scopeId, byte[] ns, byte[] key) {
        return stateGetMany(scopeId, ns, List.of(key)).get(0);
    }

    /**
     * Single-key convenience over {@link #statePutMany}.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param key     the key to upsert
     * @param value   the bytes to store
     */
    default void statePut(byte[] scopeId, byte[] ns, byte[] key, byte[] value) {
        statePutMany(scopeId, ns, List.of(new KV(key, value)));
    }
}
