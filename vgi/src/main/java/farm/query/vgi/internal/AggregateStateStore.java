// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.storage.FrameworkNs;
import farm.query.vgi.storage.FunctionStorage;

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
 * <p>Layout is byte-identical to vgi-python: {@code scope_id} is the execution
 * id, everything lives in {@link FrameworkNs#AGGREGATE_STATE}, per-group state
 * is keyed by {@link BoundStorage#packIntKey} of the group id, and bind-time
 * arguments sit at the synthetic group id {@code -2} (caller group ids are
 * non-negative, so the negative key can't collide). The execution id is minted
 * per bind — i.e. per aggregate function — so the function name needs no place
 * in the key shape.
 */
public final class AggregateStateStore {

    private static final byte[] NS = FrameworkNs.AGGREGATE_STATE.bytes();
    private static final byte[] ARGS_KEY = BoundStorage.packIntKey(-2);

    private final FunctionStorage storage;

    /**
     * Creates a store view over the worker's shared storage backend.
     *
     * @param storage the shared worker storage backend that persists state across
     *                parallel worker subprocesses
     */
    public AggregateStateStore(FunctionStorage storage) {
        this.storage = storage;
    }

    /**
     * Save bind-time arguments for an execution_id.
     *
     * @param executionId  the execution id minted at bind time (storage scope)
     * @param functionName the aggregate function name (namespace component)
     * @param argsIpc      the IPC-encoded bind arguments
     */
    public void saveArgs(byte[] executionId, String functionName, byte[] argsIpc) {
        storage.statePut(executionId, NS, ARGS_KEY, argsIpc);
    }

    /**
     * Load bind-time arguments for an execution_id.
     *
     * @param executionId  the execution id minted at bind time (storage scope)
     * @param functionName the aggregate function name (namespace component)
     * @return the IPC-encoded bind arguments saved by {@link #saveArgs}, or {@code null} if absent
     */
    public byte[] loadArgs(byte[] executionId, String functionName) {
        return storage.stateGet(executionId, NS, ARGS_KEY);
    }

    /**
     * Load state bytes for the given group_ids. Missing keys are absent from the map.
     *
     * @param executionId  the execution id minted at bind time (storage scope)
     * @param functionName the aggregate function name (namespace component)
     * @param gids         the group ids to fetch
     * @return per-group serialized state, in {@code gids} order, omitting groups with no saved state
     */
    public Map<Long, byte[]> loadStates(byte[] executionId, String functionName, long[] gids) {
        Map<Long, byte[]> out = new LinkedHashMap<>();
        if (gids.length == 0) return out;
        List<byte[]> keys = new ArrayList<>(gids.length);
        for (long g : gids) keys.add(BoundStorage.packIntKey(g));
        List<byte[]> values = storage.stateGetMany(executionId, NS, keys);
        for (int i = 0; i < gids.length; i++) {
            byte[] v = values.get(i);
            if (v != null) out.put(gids[i], v);
        }
        return out;
    }

    /**
     * Upsert state bytes for the given (gid → bytes) entries.
     *
     * @param executionId  the execution id minted at bind time (storage scope)
     * @param functionName the aggregate function name (namespace component)
     * @param states       per-group serialized state to write; an empty map is a no-op
     */
    public void saveStates(byte[] executionId, String functionName, Map<Long, byte[]> states) {
        if (states.isEmpty()) return;
        List<FunctionStorage.KV> items = new ArrayList<>(states.size());
        for (Map.Entry<Long, byte[]> e : states.entrySet()) {
            items.add(new FunctionStorage.KV(BoundStorage.packIntKey(e.getKey()), e.getValue()));
        }
        storage.statePutMany(executionId, NS, items);
    }

    /**
     * Delete the given (executionId, function, gid) tuples.
     *
     * @param executionId  the execution id minted at bind time (storage scope)
     * @param functionName the aggregate function name (namespace component)
     * @param gids         the group ids whose state should be removed
     */
    public void deleteStates(byte[] executionId, String functionName, long[] gids) {
        if (gids.length == 0) return;
        List<byte[]> keys = new ArrayList<>(gids.length);
        for (long g : gids) keys.add(BoundStorage.packIntKey(g));
        storage.stateDelete(executionId, NS, keys);
    }
}
