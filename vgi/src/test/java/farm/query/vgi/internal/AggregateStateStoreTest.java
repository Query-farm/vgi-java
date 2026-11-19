// Copyright 2026 Query Farm LLC - https://query.farm

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
 * args + per-group state round-trip (save/load/delete) on the Python-exact
 * layout — everything in {@code _vgi/aggregate_state}, group keys packed
 * little-endian, args at the synthetic group id {@code -2}.
 */
class AggregateStateStoreTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static AggregateStateStore store() {
        return new AggregateStateStore(new SqliteFunctionStorage(":memory:"),
                StoragePayloadSeal.forKey(b("test-signing-key")));
    }

    @Test
    void argsRoundTrip() {
        AggregateStateStore s = store();
        byte[] exec = b("exec1");
        assertNull(s.loadArgs(s.bind(exec, null)));
        s.saveArgs(s.bind(exec, null), b("args-ipc"));
        assertArrayEquals(b("args-ipc"), s.loadArgs(s.bind(exec, null)));
    }

    @Test
    void stateSaveLoadDelete() {
        AggregateStateStore s = store();
        byte[] exec = b("exec2");
        Map<Long, byte[]> states = new LinkedHashMap<>();
        states.put(1L, b("g1"));
        states.put(2L, b("g2"));
        s.saveStates(s.bind(exec, null), states);

        Map<Long, byte[]> got = s.loadStates(s.bind(exec, null), new long[] {1L, 2L, 99L});
        assertEquals(2, got.size()); // missing group 99 absent from the map
        assertArrayEquals(b("g1"), got.get(1L));
        assertArrayEquals(b("g2"), got.get(2L));

        s.deleteStates(s.bind(exec, null), new long[] {1L});
        Map<Long, byte[]> after = s.loadStates(s.bind(exec, null), new long[] {1L, 2L});
        assertEquals(1, after.size());
        assertArrayEquals(b("g2"), after.get(2L));
    }

    @Test
    void groupsIsolatedByExecutionId() {
        // The execution id is minted per bind (per aggregate function), so it —
        // not the function name — is the isolation boundary, matching Python.
        AggregateStateStore s = store();
        Map<Long, byte[]> fa = new LinkedHashMap<>();
        fa.put(1L, b("from-a"));
        s.saveStates(s.bind(b("exec-a"), null), fa);
        assertEquals(0, s.loadStates(s.bind(b("exec-b"), null), new long[] {1L}).size());
        assertArrayEquals(b("from-a"), s.loadStates(s.bind(b("exec-a"), null), new long[] {1L}).get(1L));
    }

    @Test
    void argsRowDoesNotCollideWithGroupState() {
        // Args live at the synthetic gid -2 in the same namespace as group
        // state; caller gids are non-negative so the keyspaces are disjoint.
        AggregateStateStore s = store();
        byte[] exec = b("exec4");
        s.saveArgs(s.bind(exec, null), b("args-ipc"));
        Map<Long, byte[]> states = new LinkedHashMap<>();
        states.put(0L, b("g0"));
        s.saveStates(s.bind(exec, null), states);
        assertArrayEquals(b("args-ipc"), s.loadArgs(s.bind(exec, null)));
        assertArrayEquals(b("g0"), s.loadStates(s.bind(exec, null), new long[] {0L}).get(0L));
        s.deleteStates(s.bind(exec, null), new long[] {0L});
        assertArrayEquals(b("args-ipc"), s.loadArgs(s.bind(exec, null)), "args survive group deletes");
    }
}
