// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The cache nonce fixtures must mint values that are unique across worker
 * processes, not just within one: the client's worker pool may serve two cache
 * MISSes from two different processes ({@code cache/transaction_scope.test}
 * asserts their nonces differ). The pid lives in the high 32 bits.
 */
class CacheNonceTest {

    @Test
    void nonceIsMonotonicAndCarriesPid() {
        long a = CacheFunctions.nextNonce();
        long b = CacheFunctions.nextNonce();
        assertEquals(a + 1, b, "nonce not monotonic within a process");
        long pid = ProcessHandle.current().pid();
        assertEquals(pid, a >> 32);
        assertEquals(pid, b >> 32);
    }
}
