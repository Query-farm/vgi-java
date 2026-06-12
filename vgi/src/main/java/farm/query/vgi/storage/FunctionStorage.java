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
 * {@code ns} namespaces the kind of state. Work-queue rows are addressed by
 * {@code execution_id} alone. Functions normally reach this surface through
 * the per-execution {@link BoundStorage} facade rather than directly.
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
     * A key/value pair for a batch put or a scan/drain result row.
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
     * Non-destructive ordered scan of a namespace's key/value rows.
     *
     * <p>Rows are ordered by key bytes (unsigned lexicographic / memcmp),
     * descending when {@code reverse}, restricted to the half-open range
     * {@code [start, end)} where either bound may be {@code null} (open).
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param start   inclusive lower key bound, or {@code null} for open
     * @param end     exclusive upper key bound, or {@code null} for open
     * @param reverse {@code true} for descending key order
     * @param limit   maximum rows to return; {@code <= 0} means unbounded
     * @return the matching rows in key order
     */
    List<KV> stateScan(byte[] scopeId, byte[] ns, byte[] start, byte[] end, boolean reverse, int limit);

    /**
     * Atomic destructive scan: reads every key/value row in the namespace and
     * deletes them in the same operation.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @return the drained rows in ascending key order
     */
    List<KV> stateDrain(byte[] scopeId, byte[] ns);

    /**
     * Deletes the given keys (missing keys are ignored).
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param keys    the keys to delete
     * @return the number of rows actually deleted
     */
    int stateDelete(byte[] scopeId, byte[] ns, List<byte[]> keys);

    /**
     * Deletes the half-open key range {@code [start, end)}; either bound may be
     * {@code null} (open), and both {@code null} wipes the whole namespace.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param start   inclusive lower key bound, or {@code null} for open
     * @param end     exclusive upper key bound, or {@code null} for open
     * @return the number of rows actually deleted
     */
    int stateDeleteRange(byte[] scopeId, byte[] ns, byte[] start, byte[] end);

    // --- atomic int64 counters ---

    /**
     * Reads the int64 counter at {@code (scope, ns, key)}.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param key     the counter key bytes
     * @return the counter value, or {@code 0} when absent
     */
    long stateCounterGet(byte[] scopeId, byte[] ns, byte[] key);

    /**
     * Atomically adds {@code delta} to the counter (initializing an absent
     * counter to 0 first) and returns the new value. The read-add-return is a
     * single backend operation — callers never need a compare-and-set loop.
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param key     the counter key bytes
     * @param delta   the (possibly negative) amount to add
     * @return the counter value after the add
     */
    long stateCounterAdd(byte[] scopeId, byte[] ns, byte[] key, long delta);

    /**
     * Overwrites the counter with {@code value} (upsert).
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param key     the counter key bytes
     * @param value   the value to store
     */
    void stateCounterSet(byte[] scopeId, byte[] ns, byte[] key, long value);

    /**
     * Deletes the counter (no-op when absent).
     *
     * @param scopeId the scope id (execution_id or transaction opaque data)
     * @param ns      the namespace bytes for this kind of state
     * @param key     the counter key bytes
     */
    void stateCounterDelete(byte[] scopeId, byte[] ns, byte[] key);

    // --- work queue (FIFO, destructive pop) ---

    /**
     * Appends work items to the execution's FIFO queue. There is no
     * registration step — a queue exists as soon as something is pushed.
     *
     * @param executionId the execution whose queue receives the items
     * @param items       the work-item payloads, in push order
     * @return the number of items pushed (i.e. {@code items.size()})
     */
    int queuePush(byte[] executionId, List<byte[]> items);

    /**
     * Atomically claims the oldest work item from the execution's queue.
     *
     * @param executionId the execution whose queue to pop
     * @return the claimed payload, or {@code null} when the queue is empty or
     *     the execution_id was never pushed (no distinct "unregistered" state,
     *     matching the Durable Object)
     */
    byte[] queuePop(byte[] executionId);

    /**
     * Removes all remaining work items for the execution.
     *
     * @param executionId the execution whose queue to clear
     * @return the number of items removed
     */
    int queueClear(byte[] executionId);

    // --- lifecycle ---

    /**
     * Wipes all state, log, and counter rows for {@code scopeId} across every
     * namespace. Work-queue rows are NOT touched (clear those via
     * {@link #queueClear}).
     *
     * @param scopeId the scope id whose state to clear
     * @return the number of rows removed across the three state tables
     */
    int executionClear(byte[] scopeId);

    /** Releases backend resources; the default is a no-op. */
    @Override
    default void close() {}

    // --- shard routing ---

    /**
     * Whether this backend routes by shard key and therefore refuses unsharded
     * use. {@link BoundStorage} consults this when no attach identity is
     * available to derive a shard from.
     *
     * @return {@code true} when a shard key is mandatory (the distributed
     *     tier); the default is {@code false}
     */
    default boolean requiresShardKey() {
        return false;
    }

    /**
     * Returns a view of this backend pinned to one shard key. Local backends
     * have no shard routing and return {@code this}.
     *
     * @param shardKey the shard key (e.g. {@code att-<hex>} from
     *     {@link ShardKey#derive})
     * @return a backend view routing every operation to that shard
     */
    default FunctionStorage forShard(String shardKey) {
        return this;
    }

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
