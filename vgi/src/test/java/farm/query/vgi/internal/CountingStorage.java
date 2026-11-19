// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.storage.FunctionStorage;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link FunctionStorage} wrapper that counts calls per method (one call is
 * one round trip on a remote backend). Shard views share the counters.
 */
final class CountingStorage implements FunctionStorage {

    private final FunctionStorage delegate;
    private final Map<String, AtomicLong> counts;

    CountingStorage(FunctionStorage delegate) {
        this(delegate, new ConcurrentHashMap<>());
    }

    private CountingStorage(FunctionStorage delegate, Map<String, AtomicLong> counts) {
        this.delegate = delegate;
        this.counts = counts;
    }

    private void hit(String m) {
        counts.computeIfAbsent(m, k -> new AtomicLong()).incrementAndGet();
    }

    long total() {
        return counts.values().stream().mapToLong(AtomicLong::get).sum();
    }

    long count(String method) {
        AtomicLong n = counts.get(method);
        return n == null ? 0 : n.get();
    }

    Map<String, Long> snapshot() {
        Map<String, Long> out = new TreeMap<>();
        counts.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    void reset() {
        counts.clear();
    }

    @Override public long stateAppend(byte[] s, byte[] ns, byte[] k, byte[] v) {
        hit("stateAppend");
        return delegate.stateAppend(s, ns, k, v);
    }

    @Override public List<LogEntry> stateLogScan(byte[] s, byte[] ns, byte[] k, long after, int limit) {
        hit("stateLogScan");
        return delegate.stateLogScan(s, ns, k, after, limit);
    }

    @Override public List<byte[]> stateGetMany(byte[] s, byte[] ns, List<byte[]> keys) {
        hit("stateGetMany");
        return delegate.stateGetMany(s, ns, keys);
    }

    @Override public void statePutMany(byte[] s, byte[] ns, List<KV> items) {
        hit("statePutMany");
        delegate.statePutMany(s, ns, items);
    }

    @Override public List<KV> stateScan(byte[] s, byte[] ns, byte[] a, byte[] b, boolean r, int l) {
        hit("stateScan");
        return delegate.stateScan(s, ns, a, b, r, l);
    }

    @Override public List<KV> stateDrain(byte[] s, byte[] ns) {
        hit("stateDrain");
        return delegate.stateDrain(s, ns);
    }

    @Override public int stateDelete(byte[] s, byte[] ns, List<byte[]> keys) {
        hit("stateDelete");
        return delegate.stateDelete(s, ns, keys);
    }

    @Override public int stateDeleteRange(byte[] s, byte[] ns, byte[] a, byte[] b) {
        hit("stateDeleteRange");
        return delegate.stateDeleteRange(s, ns, a, b);
    }

    @Override public long stateCounterGet(byte[] s, byte[] ns, byte[] k) {
        hit("stateCounterGet");
        return delegate.stateCounterGet(s, ns, k);
    }

    @Override public long stateCounterAdd(byte[] s, byte[] ns, byte[] k, long d) {
        hit("stateCounterAdd");
        return delegate.stateCounterAdd(s, ns, k, d);
    }

    @Override public void stateCounterSet(byte[] s, byte[] ns, byte[] k, long v) {
        hit("stateCounterSet");
        delegate.stateCounterSet(s, ns, k, v);
    }

    @Override public void stateCounterDelete(byte[] s, byte[] ns, byte[] k) {
        hit("stateCounterDelete");
        delegate.stateCounterDelete(s, ns, k);
    }

    @Override public int queuePush(byte[] e, List<byte[]> items) {
        hit("queuePush");
        return delegate.queuePush(e, items);
    }

    @Override public byte[] queuePop(byte[] e) {
        hit("queuePop");
        return delegate.queuePop(e);
    }

    @Override public int queueClear(byte[] e) {
        hit("queueClear");
        return delegate.queueClear(e);
    }

    @Override public int executionClear(byte[] s) {
        hit("executionClear");
        return delegate.executionClear(s);
    }

    @Override public boolean requiresShardKey() {
        return delegate.requiresShardKey();
    }

    @Override public FunctionStorage forShard(String shardKey) {
        FunctionStorage pinned = delegate.forShard(shardKey);
        return pinned == delegate ? this : new CountingStorage(pinned, counts);
    }

    @Override public void close() {
        try {
            delegate.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
