// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Render contract for {@link CacheControl}: the {@code vgi.cache.*} keys the C++
 * extension reads off each batch's {@code custom_metadata}.
 */
class CacheControlTest {

    @Test
    void ttlRendersTtlAndDefaultScope() {
        Map<String, String> md = CacheControl.ttl(300).toMetadata();
        assertEquals("300", md.get(CacheControl.TTL_KEY));
        assertEquals(CacheControl.SCOPE_CATALOG, md.get(CacheControl.SCOPE_KEY));
        assertFalse(md.containsKey(CacheControl.NO_STORE_KEY));
        assertFalse(md.containsKey(CacheControl.REVALIDATABLE_KEY));
    }

    @Test
    void noStoreOmitsFreshnessAndSetsFlag() {
        Map<String, String> md = CacheControl.noStore().toMetadata();
        assertEquals("1", md.get(CacheControl.NO_STORE_KEY));
        assertFalse(md.containsKey(CacheControl.TTL_KEY));
    }

    @Test
    void unknownScopeIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheControl.builder().scope("session").build());
    }

    @Test
    void negativeDurationsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> CacheControl.ttl(-1));
        assertThrows(IllegalArgumentException.class,
                () -> CacheControl.builder().staleWhileRevalidate(-1).build());
        assertThrows(IllegalArgumentException.class,
                () -> CacheControl.builder().staleIfError(-1).build());
    }

    @Test
    void validatorsAndStaleWindowsRender() {
        Map<String, String> md = CacheControl.builder()
                .ttl(10)
                .etag("\"rev-v1\"")
                .lastModified("2025-01-01T00:00:00Z")
                .expires("2026-01-01T00:00:00Z")
                .revalidatable(true)
                .staleWhileRevalidate(5)
                .staleIfError(7)
                .build()
                .toMetadata();
        assertEquals("\"rev-v1\"", md.get(CacheControl.ETAG_KEY));
        assertEquals("2025-01-01T00:00:00Z", md.get(CacheControl.LAST_MODIFIED_KEY));
        assertEquals("2026-01-01T00:00:00Z", md.get(CacheControl.EXPIRES_KEY));
        assertEquals("1", md.get(CacheControl.REVALIDATABLE_KEY));
        assertEquals("5", md.get(CacheControl.STALE_WHILE_REVALIDATE_KEY));
        assertEquals("7", md.get(CacheControl.STALE_IF_ERROR_KEY));
        assertFalse(md.containsKey(CacheControl.NOT_MODIFIED_KEY));
    }

    /**
     * Every boolean opt-in shares one contract: absent unless explicitly set,
     * {@code "1"} when set, and additive to the freshness keys (setting it never
     * displaces the whole-result {@code ttl}). {@code per_value} in particular
     * defaults OFF — the engine will not memoize per value without the
     * advertisement.
     *
     * @param key    the rendered {@code vgi.cache.*} key under test
     * @param setter names the builder method that turns the flag on
     */
    @ParameterizedTest(name = "{0} defaults off and renders \"1\" when set")
    @CsvSource({
        "vgi.cache.no_store,       noStore",
        "vgi.cache.revalidatable,  revalidatable",
        "vgi.cache.not_modified,   notModified",
        "vgi.cache.partition_scope,partitionScope",
        "vgi.cache.per_value,      perValue",
    })
    void booleanOptInsDefaultOffAndRenderAsOne(String key, String setter) {
        BiFunction<CacheControl.Builder, Boolean, CacheControl.Builder> apply = switch (setter) {
            case "noStore" -> CacheControl.Builder::noStore;
            case "revalidatable" -> CacheControl.Builder::revalidatable;
            case "notModified" -> CacheControl.Builder::notModified;
            case "partitionScope" -> CacheControl.Builder::partitionScope;
            case "perValue" -> CacheControl.Builder::perValue;
            default -> throw new IllegalArgumentException("unmapped setter " + setter);
        };

        // Default: the key is absent entirely, not rendered as "0".
        assertFalse(CacheControl.ttl(300).toMetadata().containsKey(key),
                key + " must be absent unless advertised");

        // Explicitly false is still absent.
        assertFalse(apply.apply(CacheControl.builder().ttl(300), false)
                        .build().toMetadata().containsKey(key),
                key + " must be absent when set false");

        // Set: "1", and additive — the freshness keys still ride along.
        Map<String, String> md = apply.apply(CacheControl.builder().ttl(300), true)
                .build().toMetadata();
        assertEquals("1", md.get(key));
        assertEquals(CacheControl.SCOPE_CATALOG, md.get(CacheControl.SCOPE_KEY));
        if (!CacheControl.NO_STORE_KEY.equals(key)) {
            assertEquals("300", md.get(CacheControl.TTL_KEY));
        }
    }

    @Test
    void perValueIsIndependentOfPartitionScope() {
        Map<String, String> md = CacheControl.builder()
                .ttl(300)
                .perValue(true)
                .build()
                .toMetadata();
        assertEquals("1", md.get(CacheControl.PER_VALUE_KEY));
        assertFalse(md.containsKey(CacheControl.PARTITION_SCOPE_KEY));

        md = CacheControl.builder()
                .ttl(300)
                .partitionScope(true)
                .build()
                .toMetadata();
        assertEquals("1", md.get(CacheControl.PARTITION_SCOPE_KEY));
        assertFalse(md.containsKey(CacheControl.PER_VALUE_KEY));
    }

    @Test
    void mergeFoldsCacheKeysIntoCallerMetadata() {
        Map<String, String> merged = CacheControl.merge(
                Map.of("vgi.partition_values", "{}"),
                CacheControl.builder().ttl(300).perValue(true).build());
        assertEquals("{}", merged.get("vgi.partition_values"));
        assertEquals("1", merged.get(CacheControl.PER_VALUE_KEY));
        assertEquals("300", merged.get(CacheControl.TTL_KEY));
    }

    @Test
    void mergeWithNullCacheControlPassesMetadataThrough() {
        Map<String, String> caller = Map.of("k", "v");
        assertTrue(caller == CacheControl.merge(caller, null));
    }
}
