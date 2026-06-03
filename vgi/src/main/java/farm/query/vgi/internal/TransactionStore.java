// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.table.TransactionStorage;

import java.nio.charset.StandardCharsets;

/**
 * Per-transaction key/value store, keyed by {@code transaction_opaque_data} and
 * backed by the worker's shared {@link FunctionStorage}.
 * {@code catalog_transaction_begin} marks a transaction active; {@code commit} /
 * {@code rollback} clear it. A {@link #view} over an active transaction is
 * handed to table-function {@code onBind} via
 * {@link farm.query.vgi.table.TableBindParams#transactionStorage()}.
 *
 * <p>Because it rides on {@link FunctionStorage}, the transaction lifecycle and
 * its cached values live in whichever tier {@code VGI_WORKER_SHARED_STORAGE}
 * selected — so a pooled multi-process / multi-replica deployment sees a
 * consistent transaction store, not just the single-process stdio launcher.
 *
 * <p>Layout: the {@code scope_id} is the transaction id; an active marker lives
 * under {@link #ACTIVE_NS} and the user key/value pairs under {@link #DATA_NS}.
 * {@code end} wipes the whole scope (marker + data) via {@code executionClear}.
 */
public final class TransactionStore {

    private static final byte[] ACTIVE_NS = "__txn_active__".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_NS = "__txn_kv__".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MARKER_KEY = {0};
    private static final byte[] MARKER_VAL = {1};

    private final FunctionStorage storage;

    /**
     * @param storage the worker's shared state backend the transaction lifecycle rides on
     */
    public TransactionStore(FunctionStorage storage) {
        this.storage = storage;
    }

    /**
     * Mark {@code transactionId} active (begin a fresh, empty store).
     *
     * @param transactionId the plain transaction id (scope key)
     */
    public void begin(byte[] transactionId) {
        storage.statePut(transactionId, ACTIVE_NS, MARKER_KEY, MARKER_VAL);
    }

    /**
     * Clear {@code transactionId}'s marker + data (commit or rollback).
     *
     * @param transactionId the plain transaction id; a {@code null} is ignored
     */
    public void end(byte[] transactionId) {
        if (transactionId != null) storage.executionClear(transactionId);
    }

    /**
     * A {@link TransactionStorage} view over {@code transactionId}, or
     * {@code null} when it is null/empty (autocommit) or not active — fixtures
     * treat {@code null} as "no caching".
     *
     * @param transactionId the plain transaction id (may be {@code null}/empty)
     * @return a storage view, or {@code null} when the id is null/empty or not active
     */
    public TransactionStorage view(byte[] transactionId) {
        if (transactionId == null || transactionId.length == 0) return null;
        if (storage.stateGet(transactionId, ACTIVE_NS, MARKER_KEY) == null) return null;
        final byte[] scope = transactionId;
        return new TransactionStorage() {
            @Override public byte[] getOne(byte[] key) { return storage.stateGet(scope, DATA_NS, key); }
            @Override public void putOne(byte[] key, byte[] value) {
                storage.statePut(scope, DATA_NS, key, value);
            }
        };
    }
}
