// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import farm.query.vgi.storage.SqliteFunctionStorage;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests the aggregate state adapter over a shared {@link SqliteFunctionStorage}:
 * args + per-group state round-trip (save/load/delete), and groups are isolated
 * by function name.
 */
class AggregateStateStoreTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static AggregateStateStore store() {
        return new AggregateStateStore(new SqliteFunctionStorage(":memory:"));
    }

    @Test
    void argsRoundTrip() {
        AggregateStateStore s = store();
        byte[] exec = b("exec1");
        assertNull(s.loadArgs(exec, "fn"));
        s.saveArgs(exec, "fn", b("args-ipc"));
        assertArrayEquals(b("args-ipc"), s.loadArgs(exec, "fn"));
    }

    @Test
    void stateSaveLoadDelete() {
        AggregateStateStore s = store();
        byte[] exec = b("exec2");
        Map<Long, byte[]> states = new LinkedHashMap<>();
        states.put(1L, b("g1"));
        states.put(2L, b("g2"));
        s.saveStates(exec, "fn", states);

        Map<Long, byte[]> got = s.loadStates(exec, "fn", new long[] {1L, 2L, 99L});
        assertEquals(2, got.size()); // missing group 99 absent from the map
        assertArrayEquals(b("g1"), got.get(1L));
        assertArrayEquals(b("g2"), got.get(2L));

        s.deleteStates(exec, "fn", new long[] {1L});
        Map<Long, byte[]> after = s.loadStates(exec, "fn", new long[] {1L, 2L});
        assertEquals(1, after.size());
        assertArrayEquals(b("g2"), after.get(2L));
    }

    @Test
    void groupsIsolatedByFunctionName() {
        AggregateStateStore s = store();
        byte[] exec = b("exec3");
        Map<Long, byte[]> fa = new LinkedHashMap<>();
        fa.put(1L, b("from-a"));
        s.saveStates(exec, "fnA", fa);
        // Same execution + same group id, different function → no collision.
        assertEquals(0, s.loadStates(exec, "fnB", new long[] {1L}).size());
        assertArrayEquals(b("from-a"), s.loadStates(exec, "fnA", new long[] {1L}).get(1L));
    }
}
