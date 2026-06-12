// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import farm.query.vgi.table.TransactionStorage;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Facade-conformance suite for {@link BoundStorage}, ported from vgi-python's
 * tests/test_bound_storage_conformance.py. Subclasses supply the backend; the
 * facade is always constructed with a 16-byte attach UUID so the suite runs
 * identically on shard-requiring backends.
 */
abstract class BoundStorageConformanceTest {

    static final byte[] ATTACH = attach((byte) 0xA1);

    protected FunctionStorage backend;

    abstract FunctionStorage createBackend() throws Exception;

    @BeforeEach
    void initBackend() throws Exception {
        backend = createBackend();
    }

    @AfterEach
    void closeBackend() throws Exception {
        if (backend != null) {
            backend.close();
        }
        afterBackendClosed();
    }

    void afterBackendClosed() throws Exception {}

    static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static byte[] attach(byte fill) {
        byte[] a = new byte[20];
        java.util.Arrays.fill(a, 0, 16, fill);
        return a;
    }

    BoundStorage bound(String exec) {
        return new BoundStorage(backend, b(exec), ATTACH);
    }

    private static final byte[] NS = b("ns");

    // --- scoping ---

    @Test
    void scopeIsolationBetweenExecutionIds() {
        BoundStorage a = bound("exec-a");
        BoundStorage c = bound("exec-c");
        a.statePut(NS, b("k"), b("from-a"));
        assertNull(c.stateGet(NS, b("k")));
        assertArrayEquals(b("from-a"), a.stateGet(NS, b("k")));
    }

    @Test
    void sameExecutionIdSharesState() {
        bound("exec-1").statePut(NS, b("k"), b("v"));
        assertArrayEquals(b("v"), bound("exec-1").stateGet(NS, b("k")));
    }

    // --- namespace coercion ---

    @Test
    void reservedNsPrefixRejected() {
        BoundStorage s = bound("exec-ns");
        assertThrows(IllegalArgumentException.class,
                () -> s.statePut(b("_vgi/anything"), b("k"), b("v")));
        assertThrows(IllegalArgumentException.class,
                () -> s.counterAdd(b("_vgi/counts"), b("k"), 1));
        assertThrows(IllegalArgumentException.class,
                () -> s.stateAppend(b("_vgi/log"), b("k"), b("v")));
    }

    @Test
    void frameworkNsMembersPassThroughAndDistinct() {
        BoundStorage s = bound("exec-fns");
        Set<String> seen = new HashSet<>();
        for (FrameworkNs ns : FrameworkNs.values()) {
            String name = new String(ns.bytes(), StandardCharsets.UTF_8);
            assertTrue(name.startsWith("_vgi/"), name);
            assertTrue(seen.add(name), "distinct: " + name);
            s.statePut(ns, b("k"), b(name));
        }
        for (FrameworkNs ns : FrameworkNs.values()) {
            assertArrayEquals(ns.bytes().length > 0
                    ? new String(ns.bytes(), StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8)
                    : null, s.stateGet(ns, b("k")));
        }
    }

    // --- state through the facade ---

    @Test
    void stateGetPutAndMany() {
        BoundStorage s = bound("exec-kv");
        s.statePutMany(NS, List.of(
                new FunctionStorage.KV(b("a"), b("1")),
                new FunctionStorage.KV(b("b"), b("2"))));
        List<byte[]> got = s.stateGetMany(NS, List.of(b("a"), b("missing"), b("b")));
        assertArrayEquals(b("1"), got.get(0));
        assertNull(got.get(1));
        assertArrayEquals(b("2"), got.get(2));
    }

    @Test
    void stateScanRangeReverseLimitThroughFacade() {
        BoundStorage s = bound("exec-scan");
        for (String k : List.of("a", "b", "c", "d", "e")) {
            s.statePut(NS, b(k), b("v:" + k));
        }
        assertEquals(5, s.stateScan(NS, null, null, false, 0).size());
        List<FunctionStorage.KV> range = s.stateScan(NS, b("b"), b("e"), false, 0);
        assertEquals(3, range.size());
        assertArrayEquals(b("b"), range.get(0).key());
        List<FunctionStorage.KV> rev = s.stateScan(NS, null, null, true, 2);
        assertEquals(2, rev.size());
        assertArrayEquals(b("e"), rev.get(0).key());
        assertArrayEquals(b("d"), rev.get(1).key());
    }

    @Test
    void stateDrainReadsAndClears() {
        BoundStorage s = bound("exec-drain");
        s.statePut(NS, b("x"), b("1"));
        s.statePut(NS, b("y"), b("2"));
        List<FunctionStorage.KV> drained = s.stateDrain(NS);
        assertEquals(2, drained.size());
        assertTrue(s.stateScan(NS, null, null, false, 0).isEmpty());
    }

    @Test
    void stateDeleteKeysRangeAndAll() {
        BoundStorage s = bound("exec-del");
        for (String k : List.of("a", "b", "c", "d", "e")) {
            s.statePut(NS, b(k), b("v"));
        }
        assertEquals(1, s.stateDelete(NS, List.of(b("a"))));
        assertEquals(2, s.stateDeleteRange(NS, b("b"), b("d")));
        assertEquals(2, s.stateDeleteRange(NS, null, null));
        assertTrue(s.stateScan(NS, null, null, false, 0).isEmpty());
    }

    @Test
    void appendLogScanCursor() {
        BoundStorage s = bound("exec-log");
        long o1 = s.stateAppend(NS, b("k"), b("a"));
        long o2 = s.stateAppend(NS, b("k"), b("b"));
        assertTrue(o1 < o2);
        List<FunctionStorage.LogEntry> all = s.stateLogScan(NS, b("k"), 0, 0);
        assertEquals(2, all.size());
        List<FunctionStorage.LogEntry> tail = s.stateLogScan(NS, b("k"), o1, 0);
        assertEquals(1, tail.size());
        assertArrayEquals(b("b"), tail.get(0).value());
    }

    // --- counters ---

    @Test
    void counterFacadeRoundtrip() {
        BoundStorage s = bound("exec-counter");
        assertEquals(0, s.counterGet(NS, b("n")));
        assertEquals(5, s.counterAdd(NS, b("n"), 5));
        assertEquals(3, s.counterAdd(NS, b("n"), -2));
        s.counterSet(NS, b("n"), 100);
        assertEquals(100, s.counterGet(NS, b("n")));
        s.counterDelete(NS, b("n"));
        assertEquals(0, s.counterGet(NS, b("n")));
    }

    // --- lifecycle ---

    @Test
    void executionClearWipesStateLogCountersScoped() {
        BoundStorage s = bound("exec-clear");
        BoundStorage other = bound("exec-other");
        s.statePut(NS, b("k"), b("v"));
        s.stateAppend(NS, b("log"), b("entry"));
        s.counterSet(NS, b("n"), 1);
        other.statePut(NS, b("k"), b("survives"));

        assertEquals(3, s.executionClear());

        assertNull(s.stateGet(NS, b("k")));
        assertTrue(s.stateLogScan(NS, b("log"), 0, 0).isEmpty());
        assertEquals(0, s.counterGet(NS, b("n")));
        assertArrayEquals(b("survives"), other.stateGet(NS, b("k")));
    }

    // --- transactions ---

    @Test
    void transactionViewRoundtripIsolationAndClear() {
        BoundStorage s = bound("exec-txn");
        TransactionStorage t1 = s.transaction(b("txn-1"));
        TransactionStorage t2 = s.transaction(b("txn-2"));
        assertNull(t1.getOne(b("watermark")));
        t1.putOne(b("watermark"), b("42"));
        assertArrayEquals(b("42"), t1.getOne(b("watermark")));
        assertNull(t2.getOne(b("watermark")), "transactions are isolated");

        t1.putMany(List.of(new FunctionStorage.KV(b("x"), b("1"))));
        List<byte[]> got = t1.getMany(List.of(b("watermark"), b("x"), b("missing")));
        assertArrayEquals(b("42"), got.get(0));
        assertArrayEquals(b("1"), got.get(1));
        assertNull(got.get(2));

        assertEquals(2, t1.clear());
        assertNull(t1.getOne(b("watermark")));
    }

    // --- queue ---

    @Test
    void queueFacadeFifo() {
        BoundStorage s = bound("exec-q");
        assertNull(s.queuePop());
        assertEquals(2, s.queuePush(List.of(b("first"), b("second"))));
        assertArrayEquals(b("first"), s.queuePop());
        assertEquals(1, s.queueClear());
        assertNull(s.queuePop());
    }

    @Test
    void queueBatchRoundtrip() {
        BoundStorage s = bound("exec-qb");
        try (RootAllocator alloc = new RootAllocator()) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of(
                            org.apache.arrow.vector.types.pojo.Field.nullable("v",
                                    new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)))),
                    alloc)) {
                IntVector v = (IntVector) root.getVector("v");
                v.allocateNew(3);
                v.set(0, 7);
                v.set(1, 8);
                v.set(2, 9);
                root.setRowCount(3);
                assertEquals(1, s.queuePushBatches(List.of(root)));
            }
            try (VectorSchemaRoot popped = s.queuePopBatch(alloc)) {
                assertNotNull(popped);
                assertEquals(3, popped.getRowCount());
                assertEquals(9, ((IntVector) popped.getVector("v")).get(2));
            }
            assertNull(s.queuePopBatch(alloc));
        }
    }

    // --- statics ---

    @Test
    void packIntKey() {
        assertArrayEquals(new byte[] {5, 0, 0, 0, 0, 0, 0, 0}, BoundStorage.packIntKey(5));
        byte[] minusOne = new byte[8];
        java.util.Arrays.fill(minusOne, (byte) 0xFF);
        assertArrayEquals(minusOne, BoundStorage.packIntKey(-1));
        assertArrayEquals(new byte[] {(byte) 0xFE, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, BoundStorage.packIntKey(-2));
    }

    @Test
    void serializeDeserializeRecordBatch() {
        try (RootAllocator alloc = new RootAllocator()) {
            byte[] data;
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of(
                            org.apache.arrow.vector.types.pojo.Field.nullable("v",
                                    new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true)))),
                    alloc)) {
                org.apache.arrow.vector.BigIntVector v =
                        (org.apache.arrow.vector.BigIntVector) root.getVector("v");
                v.allocateNew(2);
                v.set(0, 123L);
                v.set(1, 456L);
                root.setRowCount(2);
                data = BoundStorage.serializeRecordBatch(root);
            }
            try (VectorSchemaRoot back = BoundStorage.deserializeRecordBatch(data, alloc)) {
                assertEquals(2, back.getRowCount());
                assertEquals(456L,
                        ((org.apache.arrow.vector.BigIntVector) back.getVector("v")).get(1));
            }
        }
    }
}
