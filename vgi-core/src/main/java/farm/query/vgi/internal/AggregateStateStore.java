// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgi.storage.FunctionStorage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-execution aggregate state, expressed on top of the worker's shared
 * {@link FunctionStorage}. DuckDB spawns multiple worker subprocesses for
 * parallel aggregation, so partial states accumulated in worker A's heap need
 * to be visible to worker B's combine call — the shared backend (in-process
 * {@code :memory:}, local file, or a Cloudflare Durable Object) provides that.
 *
 * <p>Mapping onto {@code (scope_id, ns, key)}: {@code scope_id} is the
 * execution id; per-group state lives under {@link #stateNs} keyed by the
 * group id (8-byte big-endian); bind-time arguments live under {@link #argsNs}
 * at a fixed key. Namespacing by function name keeps multiple aggregates in
 * one execution from colliding.
 */
public final class AggregateStateStore {

    private static final byte[] ARGS_KEY = "args".getBytes(StandardCharsets.UTF_8);

    private final FunctionStorage storage;

    public AggregateStateStore(FunctionStorage storage) {
        this.storage = storage;
    }

    private static byte[] stateNs(String fn) {
        return ("agg-state:" + fn).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] argsNs(String fn) {
        return ("agg-args:" + fn).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] gidKey(long gid) {
        byte[] k = new byte[8];
        for (int i = 7; i >= 0; i--) {
            k[i] = (byte) (gid & 0xff);
            gid >>>= 8;
        }
        return k;
    }

    /** Save bind-time arguments for an execution_id. */
    public void saveArgs(byte[] executionId, String functionName, byte[] argsIpc) {
        storage.statePut(executionId, argsNs(functionName), ARGS_KEY, argsIpc);
    }

    /** Load bind-time arguments for an execution_id. Returns null if absent. */
    public byte[] loadArgs(byte[] executionId, String functionName) {
        return storage.stateGet(executionId, argsNs(functionName), ARGS_KEY);
    }

    /** Load state bytes for the given group_ids. Missing keys are absent from the map. */
    public Map<Long, byte[]> loadStates(byte[] executionId, String functionName, long[] gids) {
        Map<Long, byte[]> out = new LinkedHashMap<>();
        if (gids.length == 0) return out;
        List<byte[]> keys = new ArrayList<>(gids.length);
        for (long g : gids) keys.add(gidKey(g));
        List<byte[]> values = storage.stateGetMany(executionId, stateNs(functionName), keys);
        for (int i = 0; i < gids.length; i++) {
            byte[] v = values.get(i);
            if (v != null) out.put(gids[i], v);
        }
        return out;
    }

    /** Upsert state bytes for the given (gid → bytes) entries. */
    public void saveStates(byte[] executionId, String functionName, Map<Long, byte[]> states) {
        if (states.isEmpty()) return;
        List<FunctionStorage.KV> items = new ArrayList<>(states.size());
        for (Map.Entry<Long, byte[]> e : states.entrySet()) {
            items.add(new FunctionStorage.KV(gidKey(e.getKey()), e.getValue()));
        }
        storage.statePutMany(executionId, stateNs(functionName), items);
    }

    /** Delete the given (executionId, function, gid) tuples. */
    public void deleteStates(byte[] executionId, String functionName, long[] gids) {
        if (gids.length == 0) return;
        List<byte[]> keys = new ArrayList<>(gids.length);
        for (long g : gids) keys.add(gidKey(g));
        storage.stateDelete(executionId, stateNs(functionName), keys);
    }
}
