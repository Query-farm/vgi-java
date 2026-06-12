// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

/**
 * Per-transaction key/value store handed to a table function's {@link
 * TableFunction#onBind} via {@link TableBindParams#transactionStorage()}.
 *
 * <p>Scoped to a single {@code transaction_opaque_data} — the C++ extension
 * populates {@code BindRequest.transaction_opaque_data} only when the SQL
 * statement runs inside an explicit {@code BEGIN}/{@code COMMIT} block. Outside
 * a transaction the storage handle is {@code null} and no caching is possible.
 *
 * <p>Mirrors vgi-python's {@code BindParams.transaction_storage}. The backing
 * map is cleared by {@code catalog_transaction_commit} / {@code _rollback}.
 */
public interface TransactionStorage {

    /**
     * Looks up a value previously stored under {@code key}.
     *
     * @param key the lookup key, compared by byte content
     * @return the stored value, or {@code null} if absent
     */
    byte[] getOne(byte[] key);

    /**
     * Stores {@code value} under {@code key}, overwriting any previous value.
     *
     * @param key the key, compared by byte content
     * @param value the value to store; retrievable until the transaction
     *     commits or rolls back
     */
    void putOne(byte[] key, byte[] value);

    /**
     * Batch lookup of values previously stored in this transaction.
     *
     * @param keys the lookup keys, compared by byte content
     * @return one element per key, {@code null} where absent, in request order
     */
    java.util.List<byte[]> getMany(java.util.List<byte[]> keys);

    /**
     * Batch store of key/value pairs, overwriting any previous values.
     *
     * @param items the pairs to store; retrievable until the transaction
     *     commits or rolls back
     */
    void putMany(java.util.List<farm.query.vgi.storage.FunctionStorage.KV> items);

    /**
     * Drops every value stored for this transaction.
     *
     * @return the number of rows removed
     */
    int clear();
}
