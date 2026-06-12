// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Backend-conformance suite for {@link FunctionStorage}, ported from
 * vgi-python's tests/test_function_storage_conformance.py — every backend tier
 * must satisfy identical semantics. Subclasses supply the backend.
 */
abstract class FunctionStorageConformanceTest {

    protected FunctionStorage store;

    abstract FunctionStorage createStore() throws Exception;

    @BeforeEach
    void initStore() throws Exception {
        store = createStore();
    }

    @AfterEach
    void closeStore() throws Exception {
        if (store != null) {
            store.close();
        }
        afterStoreClosed();
    }

    void afterStoreClosed() throws Exception {}

    static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final byte[] SCOPE = b("scope-1");
    private static final byte[] NS = b("ns");

    private void seed(String... keys) {
        for (String k : keys) {
            store.statePut(SCOPE, NS, b(k), b("v:" + k));
        }
    }

    private static List<String> keysOf(List<FunctionStorage.KV> rows) {
        return rows.stream().map(kv -> new String(kv.key(), StandardCharsets.UTF_8)).toList();
    }

    // --- scan ---

    @Test
    void scanOrdersByKeyAscending() {
        // Includes a high-bit key to prove unsigned (memcmp) ordering.
        store.statePut(SCOPE, NS, new byte[] {(byte) 0x80}, b("hi"));
        seed("c", "a", "b");
        List<FunctionStorage.KV> rows = store.stateScan(SCOPE, NS, null, null, false, 0);
        assertEquals(4, rows.size());
        assertEquals(List.of("a", "b", "c"), keysOf(rows.subList(0, 3)));
        assertArrayEquals(new byte[] {(byte) 0x80}, rows.get(3).key());
    }

    @Test
    void scanReverse() {
        seed("a", "b", "c");
        assertEquals(List.of("c", "b", "a"),
                keysOf(store.stateScan(SCOPE, NS, null, null, true, 0)));
    }

    @Test
    void scanHalfOpenRange() {
        seed("a", "b", "c", "d", "e");
        assertEquals(List.of("b", "c"),
                keysOf(store.stateScan(SCOPE, NS, b("b"), b("d"), false, 0)));
    }

    @Test
    void scanOpenBounds() {
        seed("a", "b", "c", "d");
        assertEquals(List.of("c", "d"),
                keysOf(store.stateScan(SCOPE, NS, b("c"), null, false, 0)));
        assertEquals(List.of("a", "b"),
                keysOf(store.stateScan(SCOPE, NS, null, b("c"), false, 0)));
    }

    @Test
    void scanLimit() {
        seed("a", "b", "c", "d", "e");
        assertEquals(List.of("a", "b", "c"),
                keysOf(store.stateScan(SCOPE, NS, null, null, false, 3)));
        assertEquals(List.of("e", "d"),
                keysOf(store.stateScan(SCOPE, NS, null, null, true, 2)));
    }

    @Test
    void scanEmptyNamespace() {
        assertTrue(store.stateScan(SCOPE, b("nothing-here"), null, null, false, 0).isEmpty());
    }

    @Test
    void scanManyRowsRoundTrips() {
        // More rows than any backend page size, exercising continuation loops.
        for (int i = 0; i < 7; i++) {
            store.statePut(SCOPE, NS, new byte[] {(byte) i}, b("v" + i));
        }
        List<FunctionStorage.KV> rows = store.stateScan(SCOPE, NS, null, null, false, 0);
        assertEquals(7, rows.size());
        for (int i = 0; i < 7; i++) {
            assertArrayEquals(new byte[] {(byte) i}, rows.get(i).key());
            assertArrayEquals(b("v" + i), rows.get(i).value());
        }
    }

    // --- drain ---

    @Test
    void stateDrainReadsAndClears() {
        seed("a", "b", "c", "d", "e");
        List<FunctionStorage.KV> drained = store.stateDrain(SCOPE, NS);
        assertEquals(List.of("a", "b", "c", "d", "e"), keysOf(drained));
        assertArrayEquals(b("v:a"), drained.get(0).value());
        assertTrue(store.stateScan(SCOPE, NS, null, null, false, 0).isEmpty());
        assertTrue(store.stateDrain(SCOPE, NS).isEmpty());
    }

    // --- delete ---

    @Test
    void deleteHalfOpenRange() {
        seed("a", "b", "c", "d", "e");
        assertEquals(2, store.stateDeleteRange(SCOPE, NS, b("b"), b("d")));
        assertEquals(List.of("a", "d", "e"),
                keysOf(store.stateScan(SCOPE, NS, null, null, false, 0)));
    }

    @Test
    void deleteRangeOpenEnd() {
        seed("a", "b", "c", "d");
        assertEquals(2, store.stateDeleteRange(SCOPE, NS, b("c"), null));
        assertEquals(List.of("a", "b"),
                keysOf(store.stateScan(SCOPE, NS, null, null, false, 0)));
    }

    @Test
    void deleteRangeIsIdempotent() {
        seed("a", "b");
        assertEquals(2, store.stateDeleteRange(SCOPE, NS, b("a"), b("z")));
        assertEquals(0, store.stateDeleteRange(SCOPE, NS, b("a"), b("z")));
    }

    @Test
    void deleteRangeBothNullWipesNamespace() {
        seed("a", "b", "c");
        store.statePut(SCOPE, b("other"), b("x"), b("keep"));
        assertEquals(3, store.stateDeleteRange(SCOPE, NS, null, null));
        assertTrue(store.stateScan(SCOPE, NS, null, null, false, 0).isEmpty());
        assertArrayEquals(b("keep"), store.stateGet(SCOPE, b("other"), b("x")));
    }

    @Test
    void deleteKeysStillWorks() {
        seed("a", "b", "c");
        assertEquals(2, store.stateDelete(SCOPE, NS, List.of(b("a"), b("c"), b("missing"))));
        assertEquals(List.of("b"), keysOf(store.stateScan(SCOPE, NS, null, null, false, 0)));
        assertEquals(0, store.stateDelete(SCOPE, NS, List.of()));
    }

    // --- counters ---

    @Test
    void counterAbsentReadsZero() {
        assertEquals(0, store.stateCounterGet(SCOPE, NS, b("nope")));
    }

    @Test
    void counterAddAccumulatesAndReturnsNew() {
        assertEquals(5, store.stateCounterAdd(SCOPE, NS, b("c"), 5));
        assertEquals(12, store.stateCounterAdd(SCOPE, NS, b("c"), 7));
        assertEquals(12, store.stateCounterGet(SCOPE, NS, b("c")));
    }

    @Test
    void counterAddNegative() {
        assertEquals(-3, store.stateCounterAdd(SCOPE, NS, b("c"), -3));
        assertEquals(1, store.stateCounterAdd(SCOPE, NS, b("c"), 4));
    }

    @Test
    void counterSetOverwrites() {
        store.stateCounterAdd(SCOPE, NS, b("c"), 10);
        store.stateCounterSet(SCOPE, NS, b("c"), 2);
        assertEquals(2, store.stateCounterGet(SCOPE, NS, b("c")));
    }

    @Test
    void counterSetOnAbsent() {
        store.stateCounterSet(SCOPE, NS, b("fresh"), 42);
        assertEquals(42, store.stateCounterGet(SCOPE, NS, b("fresh")));
    }

    @Test
    void counterDeleteResetsAndNoops() {
        store.stateCounterSet(SCOPE, NS, b("c"), 9);
        store.stateCounterDelete(SCOPE, NS, b("c"));
        assertEquals(0, store.stateCounterGet(SCOPE, NS, b("c")));
        store.stateCounterDelete(SCOPE, NS, b("c"));
        assertEquals(0, store.stateCounterGet(SCOPE, NS, b("c")));
    }

    @Test
    void countersIndependentByKey() {
        store.stateCounterAdd(SCOPE, NS, b("x"), 1);
        store.stateCounterAdd(SCOPE, NS, b("y"), 100);
        assertEquals(1, store.stateCounterGet(SCOPE, NS, b("x")));
        assertEquals(100, store.stateCounterGet(SCOPE, NS, b("y")));
    }

    @Test
    void counterSeparateFromStateKv() {
        store.statePut(SCOPE, NS, b("k"), b("opaque"));
        store.stateCounterSet(SCOPE, NS, b("k"), 7);
        assertArrayEquals(b("opaque"), store.stateGet(SCOPE, NS, b("k")));
        assertEquals(7, store.stateCounterGet(SCOPE, NS, b("k")));
        store.stateCounterDelete(SCOPE, NS, b("k"));
        assertArrayEquals(b("opaque"), store.stateGet(SCOPE, NS, b("k")));
    }

    // --- queue ---

    @Test
    void queueFifoPushPopClear() {
        byte[] exec = b("qexec");
        assertNull(store.queuePop(exec));
        assertEquals(3, store.queuePush(exec, List.of(b("1"), b("2"), b("3"))));
        assertArrayEquals(b("1"), store.queuePop(exec));
        assertArrayEquals(b("2"), store.queuePop(exec));
        assertEquals(1, store.queueClear(exec));
        assertNull(store.queuePop(exec));
        assertEquals(0, store.queueClear(exec));
    }

    // --- execution_clear ---

    @Test
    void executionClearSweepsCountersNotQueues() {
        store.statePut(SCOPE, NS, b("k"), b("v"));
        store.stateAppend(SCOPE, NS, b("log"), b("entry"));
        store.stateCounterSet(SCOPE, NS, b("n"), 5);
        store.queuePush(SCOPE, List.of(b("work")));
        store.statePut(b("other-scope"), NS, b("k"), b("survives"));

        assertEquals(3, store.executionClear(SCOPE));

        assertNull(store.stateGet(SCOPE, NS, b("k")));
        assertTrue(store.stateLogScan(SCOPE, NS, b("log"), 0, 0).isEmpty());
        assertEquals(0, store.stateCounterGet(SCOPE, NS, b("n")));
        assertArrayEquals(b("work"), store.queuePop(SCOPE));
        assertArrayEquals(b("survives"), store.stateGet(b("other-scope"), NS, b("k")));
    }
}
