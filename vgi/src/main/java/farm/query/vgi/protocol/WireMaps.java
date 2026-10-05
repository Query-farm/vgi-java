// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Canonical (key-sorted) order for the map columns of wire records.
 *
 * <p>An Arrow map column is written in the map's iteration order, and
 * {@code Map.of}/{@code HashMap} order is unspecified (for {@code Map.of} it
 * changes from one JVM to the next). {@code catalog_contents} promises that
 * every item is byte-for-byte what the per-schema RPC returns, and a client may
 * compare or cache items by their bytes, so an item must not depend on how its
 * maps happened to be built. Sorting by key makes equal maps encode to equal
 * bytes.</p>
 */
public final class WireMaps {

    private WireMaps() {}

    /**
     * A key-sorted, unmodifiable copy of {@code map}.
     *
     * @param map the map to canonicalise; {@code null} stays {@code null}
     * @param <V> the value type (null values are kept)
     * @return the entries of {@code map} in ascending key order, or {@code null}
     */
    public static <V> Map<String, V> sorted(Map<String, V> map) {
        if (map == null) return null;
        return Collections.unmodifiableMap(new LinkedHashMap<>(new TreeMap<>(map)));
    }
}
