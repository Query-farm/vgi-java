// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import farm.query.vgi.storage.SqliteFunctionStorage;
import farm.query.vgi.table.TransactionStorage;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Tests the transaction key/value store (now backed by a shared
 * {@link SqliteFunctionStorage}): a view exists only between begin and end,
 * put/get round-trips, missing keys are null, and distinct transactions are
 * isolated.
 */
class TransactionStoreTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static TransactionStore store() {
        return new TransactionStore(new SqliteFunctionStorage(":memory:"));
    }

    @Test
    void viewIsNullBeforeBeginAndAfterEnd() {
        TransactionStore txns = store();
        byte[] txn = b("txn-lifecycle");
        assertNull(txns.view(txn, null), "no view before begin");
        txns.begin(txn, null);
        assertNotNull(txns.view(txn, null), "view exists after begin");
        txns.end(txn, null);
        assertNull(txns.view(txn, null), "no view after end");
    }

    @Test
    void putGetRoundTripsAndMissingKeyIsNull() {
        TransactionStore txns = store();
        byte[] txn = b("txn-kv");
        txns.begin(txn, null);
        try {
            TransactionStorage v = txns.view(txn, null);
            assertNotNull(v);
            assertNull(v.getOne(b("watermark")), "missing key is null");
            v.putOne(b("watermark"), b("42"));
            assertArrayEquals(b("42"), v.getOne(b("watermark")));
            v.putOne(b("watermark"), b("43")); // overwrite
            assertArrayEquals(b("43"), v.getOne(b("watermark")));
        } finally {
            txns.end(txn, null);
        }
    }

    @Test
    void distinctTransactionsAreIsolated() {
        TransactionStore txns = store();
        byte[] a = b("txn-a");
        byte[] c = b("txn-c");
        txns.begin(a, null);
        txns.begin(c, null);
        try {
            txns.view(a, null).putOne(b("k"), b("from-a"));
            assertNull(txns.view(c, null).getOne(b("k")), "txn C cannot see txn A's key");
            assertArrayEquals(b("from-a"), txns.view(a, null).getOne(b("k")));
        } finally {
            txns.end(a, null);
            txns.end(c, null);
        }
    }
}
