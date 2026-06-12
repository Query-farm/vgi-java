// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.table.TransactionStorage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * The storage facade handed to function code: a {@link FunctionStorage} view
 * bound to one {@code execution_id} and shard-pinned to one logical ATTACH.
 * Mirrors vgi-python's {@code BoundStorage} ({@code params.storage}).
 *
 * <p>Construction resolves the shard once: an attach plaintext (laid out
 * {@code uuid(16) || catalog_bytes}) yields {@link ShardKey#derive} and pins the
 * backend via {@link FunctionStorage#forShard}; with no attach, a backend that
 * {@linkplain FunctionStorage#requiresShardKey() requires sharding} (the
 * Durable Object tier) is refused, while local sqlite tiers run unpinned.
 *
 * <p>Namespaces passed as raw bytes are rejected when they begin with the
 * reserved {@code _vgi/} prefix; framework code uses the {@link FrameworkNs}
 * overloads, which bypass the check.
 */
public final class BoundStorage {

    private static final byte[] RESERVED = "_vgi/".getBytes(StandardCharsets.UTF_8);

    private final FunctionStorage store;
    private final byte[] executionId;

    /**
     * Binds a backend to one execution, resolving the shard from the attach.
     *
     * @param backend         the tier selected at startup (sqlite or Durable Object)
     * @param executionId     the opaque execution id all state calls are scoped to
     * @param attachPlaintext the framework-unwrapped attach
     *     ({@code uuid(16) || catalog_bytes}); {@code null} or empty means no
     *     attach identity is available (mirroring Python's falsy check)
     * @throws IllegalStateException if no attach identity is given but the
     *     backend requires a shard key
     * @throws IllegalArgumentException if {@code attachPlaintext} is non-empty
     *     but shorter than the 16-byte attach UUID
     */
    public BoundStorage(FunctionStorage backend, byte[] executionId, byte[] attachPlaintext) {
        if (attachPlaintext != null && attachPlaintext.length > 0) {
            this.store = backend.forShard(ShardKey.derive(attachPlaintext));
        } else if (backend.requiresShardKey()) {
            throw new IllegalStateException(
                    "this storage backend shards per attach, but no attach identity is available here");
        } else {
            this.store = backend;
        }
        this.executionId = executionId;
    }

    private BoundStorage(byte[] scopeId, FunctionStorage pinnedStore) {
        this.store = pinnedStore;
        this.executionId = scopeId;
    }

    /**
     * The {@code execution_id} this view is scoped to.
     *
     * @return the opaque execution id bytes
     */
    public byte[] executionId() {
        return executionId;
    }

    /**
     * A view over the same (already shard-pinned) backend bound to a different
     * scope id — e.g. attach-scoped state that must persist across queries,
     * mirroring vgi-python fixtures' {@code BoundStorage(storage, attach_bytes)}.
     *
     * @param scopeId the scope to bind (e.g. the attach's opaque identifier)
     * @return a facade identical to this one except for the scope
     */
    public BoundStorage rescope(byte[] scopeId) {
        return new BoundStorage(scopeId, store);
    }

    private static byte[] checkNs(byte[] ns) {
        if (ns.length >= RESERVED.length) {
            boolean reserved = true;
            for (int i = 0; i < RESERVED.length; i++) {
                if (ns[i] != RESERVED[i]) {
                    reserved = false;
                    break;
                }
            }
            if (reserved) {
                throw new IllegalArgumentException(
                        "namespaces beginning with \"_vgi/\" are reserved for the framework");
            }
        }
        return ns;
    }

    // --- key/value state ---

    /**
     * Single-key get in a user namespace.
     *
     * @param ns  the namespace bytes (must not start with {@code _vgi/})
     * @param key the key to look up
     * @return the stored bytes, or {@code null} if absent
     */
    public byte[] stateGet(byte[] ns, byte[] key) {
        return store.stateGet(executionId, checkNs(ns), key);
    }

    /**
     * Single-key get in a framework namespace.
     *
     * @param ns  the framework namespace
     * @param key the key to look up
     * @return the stored bytes, or {@code null} if absent
     */
    public byte[] stateGet(FrameworkNs ns, byte[] key) {
        return store.stateGet(executionId, ns.bytes(), key);
    }

    /**
     * Batch get in a user namespace.
     *
     * @param ns   the namespace bytes (must not start with {@code _vgi/})
     * @param keys the keys to look up
     * @return one element per key, {@code null} where absent, in request order
     */
    public List<byte[]> stateGetMany(byte[] ns, List<byte[]> keys) {
        return store.stateGetMany(executionId, checkNs(ns), keys);
    }

    /**
     * Batch get in a framework namespace.
     *
     * @param ns   the framework namespace
     * @param keys the keys to look up
     * @return one element per key, {@code null} where absent, in request order
     */
    public List<byte[]> stateGetMany(FrameworkNs ns, List<byte[]> keys) {
        return store.stateGetMany(executionId, ns.bytes(), keys);
    }

    /**
     * Single-key upsert in a user namespace.
     *
     * @param ns    the namespace bytes (must not start with {@code _vgi/})
     * @param key   the key to upsert
     * @param value the bytes to store
     */
    public void statePut(byte[] ns, byte[] key, byte[] value) {
        store.statePut(executionId, checkNs(ns), key, value);
    }

    /**
     * Single-key upsert in a framework namespace.
     *
     * @param ns    the framework namespace
     * @param key   the key to upsert
     * @param value the bytes to store
     */
    public void statePut(FrameworkNs ns, byte[] key, byte[] value) {
        store.statePut(executionId, ns.bytes(), key, value);
    }

    /**
     * Batch upsert in a user namespace.
     *
     * @param ns    the namespace bytes (must not start with {@code _vgi/})
     * @param items the key/value pairs to insert or replace
     */
    public void statePutMany(byte[] ns, List<FunctionStorage.KV> items) {
        store.statePutMany(executionId, checkNs(ns), items);
    }

    /**
     * Batch upsert in a framework namespace.
     *
     * @param ns    the framework namespace
     * @param items the key/value pairs to insert or replace
     */
    public void statePutMany(FrameworkNs ns, List<FunctionStorage.KV> items) {
        store.statePutMany(executionId, ns.bytes(), items);
    }

    /**
     * Ordered scan of a user namespace; see
     * {@link FunctionStorage#stateScan} for the range/order contract.
     *
     * @param ns      the namespace bytes (must not start with {@code _vgi/})
     * @param start   inclusive lower key bound, or {@code null} for open
     * @param end     exclusive upper key bound, or {@code null} for open
     * @param reverse {@code true} for descending key order
     * @param limit   maximum rows to return; {@code <= 0} means unbounded
     * @return the matching rows in key order
     */
    public List<FunctionStorage.KV> stateScan(byte[] ns, byte[] start, byte[] end,
                                              boolean reverse, int limit) {
        return store.stateScan(executionId, checkNs(ns), start, end, reverse, limit);
    }

    /**
     * Ordered scan of a framework namespace.
     *
     * @param ns      the framework namespace
     * @param start   inclusive lower key bound, or {@code null} for open
     * @param end     exclusive upper key bound, or {@code null} for open
     * @param reverse {@code true} for descending key order
     * @param limit   maximum rows to return; {@code <= 0} means unbounded
     * @return the matching rows in key order
     */
    public List<FunctionStorage.KV> stateScan(FrameworkNs ns, byte[] start, byte[] end,
                                              boolean reverse, int limit) {
        return store.stateScan(executionId, ns.bytes(), start, end, reverse, limit);
    }

    /**
     * Atomic destructive scan of a user namespace: reads and deletes every row.
     *
     * @param ns the namespace bytes (must not start with {@code _vgi/})
     * @return the drained rows in ascending key order
     */
    public List<FunctionStorage.KV> stateDrain(byte[] ns) {
        return store.stateDrain(executionId, checkNs(ns));
    }

    /**
     * Atomic destructive scan of a framework namespace.
     *
     * @param ns the framework namespace
     * @return the drained rows in ascending key order
     */
    public List<FunctionStorage.KV> stateDrain(FrameworkNs ns) {
        return store.stateDrain(executionId, ns.bytes());
    }

    /**
     * Deletes the given keys in a user namespace (missing keys are ignored).
     *
     * @param ns   the namespace bytes (must not start with {@code _vgi/})
     * @param keys the keys to delete
     * @return the number of rows actually deleted
     */
    public int stateDelete(byte[] ns, List<byte[]> keys) {
        return store.stateDelete(executionId, checkNs(ns), keys);
    }

    /**
     * Deletes the given keys in a framework namespace.
     *
     * @param ns   the framework namespace
     * @param keys the keys to delete
     * @return the number of rows actually deleted
     */
    public int stateDelete(FrameworkNs ns, List<byte[]> keys) {
        return store.stateDelete(executionId, ns.bytes(), keys);
    }

    /**
     * Deletes the half-open key range {@code [start, end)} in a user namespace;
     * both bounds {@code null} wipes the whole namespace.
     *
     * @param ns    the namespace bytes (must not start with {@code _vgi/})
     * @param start inclusive lower key bound, or {@code null} for open
     * @param end   exclusive upper key bound, or {@code null} for open
     * @return the number of rows actually deleted
     */
    public int stateDeleteRange(byte[] ns, byte[] start, byte[] end) {
        return store.stateDeleteRange(executionId, checkNs(ns), start, end);
    }

    /**
     * Deletes the half-open key range {@code [start, end)} in a framework
     * namespace; both bounds {@code null} wipes the whole namespace.
     *
     * @param ns    the framework namespace
     * @param start inclusive lower key bound, or {@code null} for open
     * @param end   exclusive upper key bound, or {@code null} for open
     * @return the number of rows actually deleted
     */
    public int stateDeleteRange(FrameworkNs ns, byte[] start, byte[] end) {
        return store.stateDeleteRange(executionId, ns.bytes(), start, end);
    }

    // --- append-only log ---

    /**
     * Appends a value to the {@code (executionId, ns, key)} log in a user
     * namespace.
     *
     * @param ns    the namespace bytes (must not start with {@code _vgi/})
     * @param key   the log key bytes
     * @param value the bytes to append
     * @return the monotonically increasing log id assigned to the entry
     */
    public long stateAppend(byte[] ns, byte[] key, byte[] value) {
        return store.stateAppend(executionId, checkNs(ns), key, value);
    }

    /**
     * Appends a value to the log in a framework namespace.
     *
     * @param ns    the framework namespace
     * @param key   the log key bytes
     * @param value the bytes to append
     * @return the monotonically increasing log id assigned to the entry
     */
    public long stateAppend(FrameworkNs ns, byte[] key, byte[] value) {
        return store.stateAppend(executionId, ns.bytes(), key, value);
    }

    /**
     * Scans the {@code (executionId, ns, key)} log after a cursor in a user
     * namespace.
     *
     * @param ns      the namespace bytes (must not start with {@code _vgi/})
     * @param key     the log key bytes
     * @param afterId return only entries with id strictly greater than this
     * @param limit   maximum entries to return; {@code <= 0} means unbounded
     * @return the matching log entries in id order
     */
    public List<FunctionStorage.LogEntry> stateLogScan(byte[] ns, byte[] key, long afterId, int limit) {
        return store.stateLogScan(executionId, checkNs(ns), key, afterId, limit);
    }

    /**
     * Scans the log after a cursor in a framework namespace.
     *
     * @param ns      the framework namespace
     * @param key     the log key bytes
     * @param afterId return only entries with id strictly greater than this
     * @param limit   maximum entries to return; {@code <= 0} means unbounded
     * @return the matching log entries in id order
     */
    public List<FunctionStorage.LogEntry> stateLogScan(FrameworkNs ns, byte[] key, long afterId, int limit) {
        return store.stateLogScan(executionId, ns.bytes(), key, afterId, limit);
    }

    // --- atomic int64 counters ---

    /**
     * Reads a counter in a user namespace.
     *
     * @param ns  the namespace bytes (must not start with {@code _vgi/})
     * @param key the counter key
     * @return the counter value, or {@code 0} when absent
     */
    public long counterGet(byte[] ns, byte[] key) {
        return store.stateCounterGet(executionId, checkNs(ns), key);
    }

    /**
     * Reads a counter in a framework namespace.
     *
     * @param ns  the framework namespace
     * @param key the counter key
     * @return the counter value, or {@code 0} when absent
     */
    public long counterGet(FrameworkNs ns, byte[] key) {
        return store.stateCounterGet(executionId, ns.bytes(), key);
    }

    /**
     * Atomically adds {@code delta} to a counter in a user namespace and
     * returns the new value (an absent counter starts at 0).
     *
     * @param ns    the namespace bytes (must not start with {@code _vgi/})
     * @param key   the counter key
     * @param delta the (possibly negative) amount to add
     * @return the counter value after the add
     */
    public long counterAdd(byte[] ns, byte[] key, long delta) {
        return store.stateCounterAdd(executionId, checkNs(ns), key, delta);
    }

    /**
     * Atomically adds {@code delta} to a counter in a framework namespace.
     *
     * @param ns    the framework namespace
     * @param key   the counter key
     * @param delta the (possibly negative) amount to add
     * @return the counter value after the add
     */
    public long counterAdd(FrameworkNs ns, byte[] key, long delta) {
        return store.stateCounterAdd(executionId, ns.bytes(), key, delta);
    }

    /**
     * Overwrites a counter in a user namespace.
     *
     * @param ns    the namespace bytes (must not start with {@code _vgi/})
     * @param key   the counter key
     * @param value the value to store
     */
    public void counterSet(byte[] ns, byte[] key, long value) {
        store.stateCounterSet(executionId, checkNs(ns), key, value);
    }

    /**
     * Overwrites a counter in a framework namespace.
     *
     * @param ns    the framework namespace
     * @param key   the counter key
     * @param value the value to store
     */
    public void counterSet(FrameworkNs ns, byte[] key, long value) {
        store.stateCounterSet(executionId, ns.bytes(), key, value);
    }

    /**
     * Deletes a counter in a user namespace (no-op when absent).
     *
     * @param ns  the namespace bytes (must not start with {@code _vgi/})
     * @param key the counter key
     */
    public void counterDelete(byte[] ns, byte[] key) {
        store.stateCounterDelete(executionId, checkNs(ns), key);
    }

    /**
     * Deletes a counter in a framework namespace (no-op when absent).
     *
     * @param ns  the framework namespace
     * @param key the counter key
     */
    public void counterDelete(FrameworkNs ns, byte[] key) {
        store.stateCounterDelete(executionId, ns.bytes(), key);
    }

    // --- lifecycle ---

    /**
     * Wipes all state, log, and counter rows for this execution across every
     * namespace (queue items are untouched — use {@link #queueClear}).
     *
     * @return the number of rows removed
     */
    public int executionClear() {
        return store.executionClear(executionId);
    }

    // --- work queue ---

    /**
     * Appends work items to this execution's FIFO queue.
     *
     * @param items the work-item payloads, in push order
     * @return the number of items pushed
     */
    public int queuePush(List<byte[]> items) {
        return store.queuePush(executionId, items);
    }

    /**
     * Atomically claims the oldest work item from this execution's queue.
     *
     * @return the claimed payload, or {@code null} when the queue is empty
     */
    public byte[] queuePop() {
        return store.queuePop(executionId);
    }

    /**
     * Removes all remaining work items for this execution.
     *
     * @return the number of items removed
     */
    public int queueClear() {
        return store.queueClear(executionId);
    }

    /**
     * Serializes each batch via Arrow IPC and pushes them as work items.
     *
     * @param batches the batches to push (not closed by this call)
     * @return the number of items pushed
     */
    public int queuePushBatches(List<VectorSchemaRoot> batches) {
        List<byte[]> items = new ArrayList<>(batches.size());
        for (VectorSchemaRoot root : batches) {
            items.add(serializeRecordBatch(root));
        }
        return queuePush(items);
    }

    /**
     * Pops one work item and deserializes it as an Arrow batch.
     *
     * @param allocator the allocator for the batch's buffers
     * @return the batch (caller closes), or {@code null} when the queue is empty
     */
    public VectorSchemaRoot queuePopBatch(BufferAllocator allocator) {
        byte[] item = queuePop();
        return item == null ? null : deserializeRecordBatch(item, allocator);
    }

    // --- transactions ---

    /**
     * A transaction-scoped key/value view sharing this facade's shard pinning.
     *
     * @param transactionOpaqueData the transaction token used as the scope id
     * @return the transaction view (namespace {@code txn})
     */
    public TransactionStorage transaction(byte[] transactionOpaqueData) {
        return new TransactionBoundStorage(store, transactionOpaqueData);
    }

    // --- statics ---

    /**
     * Canonical integer key encoding for {@code state_*} keys: 8-byte
     * little-endian signed, byte-identical to vgi-python's {@code pack_int_key}.
     *
     * @param i the integer (e.g. a state id, partition id, or group id)
     * @return the 8-byte little-endian two's-complement encoding
     */
    public static byte[] packIntKey(long i) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(i).array();
    }

    /**
     * Serializes a batch as a single-batch Arrow IPC stream (the queue and
     * buffering wire encoding, identical to the Python helpers).
     *
     * @param root the batch to serialize (not closed by this call)
     * @return the IPC stream bytes
     */
    public static byte[] serializeRecordBatch(VectorSchemaRoot root) {
        return BatchUtil.writeSingleBatch(root);
    }

    /**
     * Deserializes a single-batch Arrow IPC stream.
     *
     * @param data      the IPC stream bytes
     * @param allocator the allocator for the batch's buffers
     * @return the batch (caller closes)
     */
    public static VectorSchemaRoot deserializeRecordBatch(byte[] data, BufferAllocator allocator) {
        return BatchUtil.readSingleBatch(data, allocator);
    }
}
