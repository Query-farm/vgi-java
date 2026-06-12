// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link CfdoStorage} against the in-process {@link MockDoServer}. The
 * mock REQUIRES a valid shard_key on every request and a 32-hex attempt_id on
 * every destructive op, so a passing run proves the client both speaks the
 * protocol and shards. Full operation-level coverage lives in the conformance
 * suites; this exercises the protocol details those can't see (paging wire
 * shape, drain attempt_id reuse, shard enforcement).
 */
class CfdoStorageTest {

    private static final String SHARD = "att-0123456789abcdef0123456789abcdef";

    private MockDoServer mock;
    private CfdoStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        mock = new MockDoServer();
        storage = new CfdoStorage(mock.baseUrl(), null).forShard(SHARD);
    }

    @AfterEach
    void tearDown() {
        mock.close();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void appendLogRoundTripsAndShards() {
        byte[] exec = b("exec1");
        byte[] ns = b("buf");
        byte[] key = b("k");
        long o1 = storage.stateAppend(exec, ns, key, b("a"));
        long o2 = storage.stateAppend(exec, ns, key, b("b"));
        assertTrue(o1 < o2, "ordinals are monotonic");

        List<CfdoStorage.LogEntry> rows = storage.stateLogScan(exec, ns, key, -1, 0);
        assertEquals(2, rows.size());
        assertArrayEquals(b("a"), rows.get(0).value());
        assertArrayEquals(b("b"), rows.get(1).value());

        List<CfdoStorage.LogEntry> tail = storage.stateLogScan(exec, ns, key, o1, 1);
        assertEquals(1, tail.size());
        assertArrayEquals(b("b"), tail.get(0).value());

        assertEquals(Set.of(SHARD), mock.shardKeys);
        assertEquals(0, mock.missingShard);
        assertEquals(0, mock.missingAttempt);
        assertTrue(mock.attemptIds.size() >= 2);
    }

    @Test
    void transactionPutGetAndClear() {
        byte[] txn = b("txn1");
        byte[] ns = b("txn");
        byte[] key = b("watermark");
        assertNull(storage.stateGet(txn, ns, key));
        storage.statePut(txn, ns, key, b("42"));
        assertArrayEquals(b("42"), storage.stateGet(txn, ns, key));
        storage.executionClear(txn);
        assertNull(storage.stateGet(txn, ns, key));
        assertEquals(0, mock.missingShard);
        assertEquals(0, mock.missingAttempt);
    }

    @Test
    void emptyShardKeyRejectedByServerContract() {
        CfdoStorage unsharded = new CfdoStorage(mock.baseUrl(), null);
        assertThrows(CfdoStorage.CfdoException.class,
                () -> unsharded.statePut(b("e"), b("n"), b("k"), b("v")));
        assertTrue(mock.missingShard > 0);
    }

    @Test
    void scanPagesAcrossMultipleRequests() {
        byte[] exec = b("scanexec");
        byte[] ns = b("ns");
        for (int i = 0; i < 5; i++) {
            storage.statePut(exec, ns, new byte[] {(byte) i}, b("v" + i));
        }
        // 5 rows at PAGE=2 forces a 3-request continuation loop.
        List<CfdoStorage.KV> all = storage.stateScan(exec, ns, null, null, false, 0);
        assertEquals(5, all.size());
        for (int i = 0; i < 5; i++) {
            assertArrayEquals(new byte[] {(byte) i}, all.get(i).key());
        }
    }

    @Test
    void drainReusesOneAttemptIdAcrossPages() {
        byte[] exec = b("drainexec");
        byte[] ns = b("ns");
        for (int i = 0; i < 5; i++) {
            storage.statePut(exec, ns, new byte[] {(byte) i}, b("v" + i));
        }
        int attemptsBefore = mock.attemptIds.size();
        List<CfdoStorage.KV> drained = storage.stateDrain(exec, ns);
        assertEquals(5, drained.size());
        // 3 pages arrived, all carrying ONE freshly minted attempt_id.
        List<String> drainAttempts = mock.attemptIds.subList(attemptsBefore, mock.attemptIds.size());
        assertEquals(3, drainAttempts.size());
        assertEquals(1, Set.copyOf(drainAttempts).size());
        assertEquals(0, mock.badDrainContinuation);
        assertTrue(storage.stateScan(exec, ns, null, null, false, 0).isEmpty());
    }
}
