// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.table.TransactionStorage;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-wide registry of per-transaction key/value stores, keyed by
 * {@code transaction_opaque_data}. {@code catalog_transaction_begin} allocates
 * an entry; {@code catalog_transaction_commit} / {@code _rollback} drop it.
 * A {@link #view} over an entry is handed to table-function {@code onBind} via
 * {@link farm.query.vgi.table.TableBindParams#transactionStorage()}.
 *
 * <p>The store is in-process: it works for the stdio / AF_UNIX (launcher)
 * transports the integration suite uses, where one long-lived worker handles
 * an attach's whole transaction lifecycle. A pooled HTTP deployment would need
 * a shared backend — out of scope here, same as vgi-go.
 */
public final class TransactionStore {

    private TransactionStore() {}

    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, byte[]>> TXNS =
            new ConcurrentHashMap<>();

    /** Register a fresh, empty store for {@code transactionId}. */
    public static void begin(byte[] transactionId) {
        TXNS.put(HexId.encode(transactionId), new ConcurrentHashMap<>());
    }

    /** Drop the store for {@code transactionId} (commit or rollback). */
    public static void end(byte[] transactionId) {
        if (transactionId != null) TXNS.remove(HexId.encode(transactionId));
    }

    /**
     * A {@link TransactionStorage} view over the entry for {@code transactionId},
     * or {@code null} when {@code transactionId} is null/empty (autocommit) or
     * the transaction is unknown — fixtures treat {@code null} as "no caching".
     */
    public static TransactionStorage view(byte[] transactionId) {
        if (transactionId == null || transactionId.length == 0) return null;
        ConcurrentHashMap<String, byte[]> map = TXNS.get(HexId.encode(transactionId));
        if (map == null) return null;
        return new TransactionStorage() {
            @Override public byte[] getOne(byte[] key) { return map.get(HexId.encode(key)); }
            @Override public void putOne(byte[] key, byte[] value) {
                map.put(HexId.encode(key), value);
            }
        };
    }
}
