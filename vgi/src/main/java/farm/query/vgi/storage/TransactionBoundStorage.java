// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import farm.query.vgi.table.TransactionStorage;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A {@link TransactionStorage} backed by the unified state surface:
 * {@code scope_id = transaction_opaque_data}, namespace {@code txn}
 * (byte-identical to vgi-python's {@code TransactionBoundStorage}). Obtain one
 * via {@link BoundStorage#transaction} (which shares the parent's shard
 * pinning) or framework-side from {@code TransactionStore}.
 */
public final class TransactionBoundStorage implements TransactionStorage {

    private static final byte[] NS = "txn".getBytes(StandardCharsets.UTF_8);

    private final FunctionStorage store;
    private final byte[] transactionOpaqueData;

    /**
     * Binds a (possibly shard-pinned) backend to one transaction.
     *
     * @param store                 the backend; pin it via
     *     {@link FunctionStorage#forShard} before constructing when sharding
     * @param transactionOpaqueData the transaction token used as the scope id
     */
    public TransactionBoundStorage(FunctionStorage store, byte[] transactionOpaqueData) {
        this.store = store;
        this.transactionOpaqueData = transactionOpaqueData;
    }

    @Override
    public byte[] getOne(byte[] key) {
        return store.stateGet(transactionOpaqueData, NS, key);
    }

    @Override
    public void putOne(byte[] key, byte[] value) {
        store.statePut(transactionOpaqueData, NS, key, value);
    }

    @Override
    public List<byte[]> getMany(List<byte[]> keys) {
        return store.stateGetMany(transactionOpaqueData, NS, keys);
    }

    @Override
    public void putMany(List<FunctionStorage.KV> items) {
        store.statePutMany(transactionOpaqueData, NS, items);
    }

    @Override
    public int clear() {
        return store.executionClear(transactionOpaqueData);
    }
}
