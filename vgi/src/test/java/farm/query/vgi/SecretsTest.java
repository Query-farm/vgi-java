// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Type- and scope-aware selection over resolved secrets. */
class SecretsTest {

    private static Map<String, String> fields(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static Secrets sample() {
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        byName.put("my_s3", fields("type", "s3", "key_id", "AAA", "scope", "s3://bucket-a"));
        byName.put("my_s3_b",
                fields("type", "s3", "key_id", "BBB", "scope", "s3://bucket-b\ns3://bucket-b2"));
        byName.put("my_gcs", fields("type", "gcs", "key_id", "G"));
        return Secrets.of(byName);
    }

    @Test
    void typeAware() {
        Secrets s = sample();
        assertEquals("s3", s.secretType("my_s3").orElse(null));
        assertEquals("gcs", s.secretType("my_gcs").orElse(null));
        assertEquals(2, s.ofType("s3").size());
        assertEquals(1, s.ofType("gcs").size());
        assertTrue(s.ofType("azure").isEmpty());
    }

    @Test
    void scopeSelectionPerBucket() {
        Secrets s = sample();
        assertEquals("AAA", s.forScopeOfType("s3://bucket-a/x.dat", "s3").orElseThrow().get("key_id"));
        assertEquals("BBB", s.forScopeOfType("s3://bucket-b2/y.dat", "s3").orElseThrow().get("key_id"));
        assertEquals("AAA", s.fieldFor("s3://bucket-a/x.dat", "key_id").orElse(null));
    }

    @Test
    void longestPrefixAndFallback() {
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        byName.put("broad", fields("type", "s3", "key_id", "broad", "scope", "s3://bucket"));
        byName.put("narrow", fields("type", "s3", "key_id", "narrow", "scope", "s3://bucket/data"));
        Secrets s = Secrets.of(byName);
        assertEquals("narrow", s.fieldFor("s3://bucket/data/x.dat", "key_id").orElse(null));
        assertEquals("broad", s.fieldFor("s3://bucket/other/x.dat", "key_id").orElse(null));

        Secrets unscoped = Secrets.of(Map.of("only", fields("type", "s3", "key_id", "only")));
        assertEquals("only", unscoped.fieldFor("s3://any/x", "key_id").orElse(null));

        assertFalse(s.forScope("s3://nope/x").isPresent());
    }

    @Test
    void parseEmpty() {
        assertTrue(Secrets.parse(new byte[0]).byName().isEmpty());
        assertTrue(Secrets.parse(null).field("anything").isEmpty());
    }
}
