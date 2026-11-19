// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.storage.FrameworkNs;
import farm.query.vgi.storage.FunctionStorage;

import java.util.ArrayList;
import java.util.HexFormat;
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
 * <p>Every call goes through a {@link BoundStorage} built from the request's
 * authenticated attach ({@link #bind}), so a sharding backend (the Durable
 * Object tier) routes the execution to its attach's shard, exactly as
 * buffering does.
 *
 * <p>Layout matches vgi-python: {@code scope_id} is the execution id, everything
 * lives in {@link FrameworkNs#AGGREGATE_STATE}, per-group state is keyed by
 * {@link BoundStorage#packIntKey} of the group id, and bind-time arguments sit
 * at the synthetic group id {@code -2} (caller group ids are non-negative, so
 * the negative key can't collide). The arguments are framework context and
 * are stored sealed ({@link StoragePayloadSeal}); group states are the
 * function's own data and are stored as its codec wrote them.
 *
 * <p>Bind-time arguments never change for an execution, so they are kept in a
 * bounded process-local cache keyed by {@code (shard, execution)} — including
 * the "no arguments" answer. Storage stays authoritative on a miss.
 */
public final class AggregateStateStore {

    private static final FrameworkNs NS = FrameworkNs.AGGREGATE_STATE;
    private static final byte[] NS_BYTES = NS.bytes();
    private static final byte[] ARGS_KEY = BoundStorage.packIntKey(-2);
    /** Cached "this execution has no bind-time arguments". */
    private static final byte[] NO_ARGS = new byte[0];
    private static final int ARGS_CACHE_ENTRIES = 1024;

    private final FunctionStorage backend;
    private final StoragePayloadSeal seal;
    private final Map<String, byte[]> argsCache = BoundedMap.createLru(ARGS_CACHE_ENTRIES);

    /**
     * Creates a store over the worker's shared backend, sealing with the host-local key.
     *
     * @param backend the shared worker storage backend
     */
    public AggregateStateStore(FunctionStorage backend) {
        this(backend, StoragePayloadSeal.forKey(null));
    }

    /**
     * Creates a store over the worker's shared backend.
     *
     * @param backend the shared worker storage backend
     * @param seal    seals the framework-written bind arguments
     */
    public AggregateStateStore(FunctionStorage backend, StoragePayloadSeal seal) {
        this.backend = backend;
        this.seal = seal;
    }

    /**
     * The storage view for one execution, shard-pinned to the request's attach.
     *
     * @param executionId the execution id minted at bind time
     * @param attachPlain the request's authenticated, unwrapped attach, or {@code null}
     * @return the bound view
     */
    public BoundStorage bind(byte[] executionId, byte[] attachPlain) {
        return new BoundStorage(backend, executionId, attachPlain);
    }

    private static String cacheKey(BoundStorage s) {
        return s.shardKey() + "|" + HexFormat.of().formatHex(s.executionId());
    }

    /**
     * Save (sealed) bind-time arguments for an execution.
     *
     * @param s       the execution's bound storage
     * @param argsIpc the IPC-encoded bind arguments
     */
    public void saveArgs(BoundStorage s, byte[] argsIpc) {
        s.statePut(NS, ARGS_KEY, seal.seal(argsIpc, s.shardKey(), s.executionId(), NS_BYTES, ARGS_KEY));
        argsCache.put(cacheKey(s), argsIpc.clone());
    }

    /**
     * Load bind-time arguments for an execution: from the process-local cache,
     * else from storage (opened and authenticated), caching the answer.
     *
     * @param s the execution's bound storage
     * @return the IPC-encoded bind arguments, or {@code null} if none were saved
     * @throws StoragePayloadSeal.StoragePayloadException if the stored value fails authentication
     */
    public byte[] loadArgs(BoundStorage s) {
        String k = cacheKey(s);
        byte[] cached = argsCache.get(k);
        if (cached != null) return cached == NO_ARGS ? null : cached;
        byte[] sealed = s.stateGet(NS, ARGS_KEY);
        byte[] plain = sealed == null ? null
                : seal.open(sealed, s.shardKey(), s.executionId(), NS_BYTES, ARGS_KEY);
        argsCache.put(k, plain == null ? NO_ARGS : plain);
        return plain;
    }

    /**
     * Load state bytes for the given group ids. Missing keys are absent from the map.
     *
     * @param s    the execution's bound storage
     * @param gids the group ids to fetch
     * @return per-group serialized state, in {@code gids} order, omitting groups with no saved state
     */
    public Map<Long, byte[]> loadStates(BoundStorage s, long[] gids) {
        Map<Long, byte[]> out = new LinkedHashMap<>();
        if (gids.length == 0) return out;
        List<byte[]> keys = new ArrayList<>(gids.length);
        for (long g : gids) keys.add(BoundStorage.packIntKey(g));
        List<byte[]> values = s.stateGetMany(NS, keys);
        for (int i = 0; i < gids.length; i++) {
            byte[] v = values.get(i);
            if (v != null) out.put(gids[i], v);
        }
        return out;
    }

    /**
     * Upsert state bytes for the given (gid → bytes) entries.
     *
     * @param s      the execution's bound storage
     * @param states per-group serialized state to write; an empty map is a no-op
     */
    public void saveStates(BoundStorage s, Map<Long, byte[]> states) {
        if (states.isEmpty()) return;
        List<FunctionStorage.KV> items = new ArrayList<>(states.size());
        for (Map.Entry<Long, byte[]> e : states.entrySet()) {
            items.add(new FunctionStorage.KV(BoundStorage.packIntKey(e.getKey()), e.getValue()));
        }
        s.statePutMany(NS, items);
    }

    /**
     * Delete the given groups' state and drop the execution's cached arguments
     * (storage stays authoritative if a later call needs them again).
     *
     * @param s    the execution's bound storage
     * @param gids the group ids whose state should be removed
     */
    public void deleteStates(BoundStorage s, long[] gids) {
        argsCache.remove(cacheKey(s));
        if (gids.length == 0) return;
        List<byte[]> keys = new ArrayList<>(gids.length);
        for (long g : gids) keys.add(BoundStorage.packIntKey(g));
        s.stateDelete(NS, keys);
    }

    /** Number of cached argument entries (tests). */
    int cachedArgsCount() {
        return argsCache.size();
    }
}
