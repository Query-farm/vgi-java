// Copyright 2025-2026 Query.Farm LLC

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

    /** One appended record from a state log: its monotonic cursor id and value. */
    record LogEntry(long id, byte[] value) {}

    /** A key/value pair for a batch put. */
    record KV(byte[] key, byte[] value) {}

    // --- append-only log (table-buffering) ---

    /** Append {@code value} to the {@code (scope, ns, key)} log; return its resumable cursor id. */
    long stateAppend(byte[] scopeId, byte[] ns, byte[] key, byte[] value);

    /** Scan up to {@code limit} entries with id &gt; {@code afterId}, in order. {@code limit <= 0} = unbounded. */
    List<LogEntry> stateLogScan(byte[] scopeId, byte[] ns, byte[] key, long afterId, int limit);

    // --- key/value (transactions, aggregate state) ---

    /** Batch get: returns one element per requested key, {@code null} where absent (order preserved). */
    List<byte[]> stateGetMany(byte[] scopeId, byte[] ns, List<byte[]> keys);

    /** Batch upsert. */
    void statePutMany(byte[] scopeId, byte[] ns, List<KV> items);

    /** Delete the given keys (missing keys ignored). */
    void stateDelete(byte[] scopeId, byte[] ns, List<byte[]> keys);

    // --- lifecycle ---

    /** Wipe all state + log rows for {@code scopeId} across every namespace; return rows removed. */
    int executionClear(byte[] scopeId);

    @Override
    default void close() {}

    // --- single-key conveniences ---

    /** Single-key get; {@code null} if absent. */
    default byte[] stateGet(byte[] scopeId, byte[] ns, byte[] key) {
        return stateGetMany(scopeId, ns, List.of(key)).get(0);
    }

    /** Single-key upsert. */
    default void statePut(byte[] scopeId, byte[] ns, byte[] key, byte[] value) {
        statePutMany(scopeId, ns, List.of(new KV(key, value)));
    }
}
