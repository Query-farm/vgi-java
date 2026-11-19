// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Insertion-ordered {@link Map} capped at a fixed entry count. When a
 * {@code put} pushes size past the cap, the eldest entry is evicted.
 *
 * <p>Designed for caches of short-lived RPC state (bind tokens, in-flight TIO
 * executions) whose normal lifecycle is bind→init→drop, but which leak under
 * cancelled queries. Capping prevents unbounded growth in long-running HTTP
 * deployments without changing the well-behaved fast path.</p>
 *
 * <p>The returned map is wrapped with {@link Collections#synchronizedMap} so
 * concurrent put/remove from RPC handlers is safe; iteration must be
 * externally synchronized.</p>
 */
final class BoundedMap {

    private BoundedMap() {}

    static <K, V> Map<K, V> create(int maxEntries) {
        return Collections.synchronizedMap(new LinkedHashMap<K, V>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > maxEntries;
            }
        });
    }

    /**
     * A thread-safe, size-bounded map in access order: the least recently used
     * entry is evicted first.
     *
     * @param maxEntries the entry cap
     * @param <K> key type
     * @param <V> value type
     * @return the synchronized LRU map
     */
    static <K, V> Map<K, V> createLru(int maxEntries) {
        return Collections.synchronizedMap(new LinkedHashMap<K, V>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > maxEntries;
            }
        });
    }
}
