// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Secret scope matching follows DuckDB's authority boundary (duckdb#28684). The
 * vectors ({@code secrets/scope_match_vectors.json}, shared by every VGI SDK)
 * were generated from DuckDB's own {@code BaseSecret::MatchScore} and
 * {@code StringUtil::GetURIAuthority} (Haybarn b2aa6ca).
 */
class SecretsScopeMatchTest {

    /** The first 25 shared vectors, inline so the rule reads without the JSON. */
    private static final String[][] CURATED = {
                {"snmp://10.0.0.1", "snmp://10.0.0.1", "true"},
                {"snmp://10.0.0.1", "snmp://10.0.0.1/x", "true"},
                {"snmp://10.0.0.1", "snmp://10.0.0.15", "false"},
                {"snmp://10.0.0.1", "snmp://10.0.0.1:161", "false"},
                {"s3://bucket", "s3://bucket/key", "true"},
                {"s3://bucket", "s3://bucket-evil/key", "false"},
                {"s3://bucket", "s3://bucket?x=1", "false"},
                {"dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture:shop", "false"},
                {"dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture/shop", "true"},
                {"s3://bucket/", "s3://bucket/key", "true"},
                {"s3://bucket/pre", "s3://bucket/prefix/x", "true"},
                {"s3://bucket/pre", "s3://bucket/other", "false"},
                {"https://h?a", "https://h?ab", "true"},
                {"https://h#f", "https://h#fx", "true"},
                {"a://b\\c", "a://b\\cd", "true"},
                {"snmp://", "snmp://anything", "true"},
                {"snmp://10.0.0.", "snmp://10.0.0.15:161", "false"},
                {"foo", "foobar", "true"},
                {"c:\\data", "c:\\data2\\x", "true"},
                {"x:/y://z", "x:/y://zz", "true"},
                {"s3://bucket", "s3://buck", "false"},
                {"s3://bucket", "s3://bucket", "true"},
                {"vgi-conformance://host1", "vgi-conformance://host1/data", "true"},
                {"vgi-conformance://host1", "vgi-conformance://host10/data", "false"},
                {"vgi-conformance://host1", "vgi-conformance://host1:9000", "false"}
    };

    @Test
    void curatedCases() {
        for (String[] c : CURATED) {
            assertEquals(Boolean.parseBoolean(c[2]), Secrets.scopeMatches(c[0], c[1]),
                    "scope " + c[0] + " path " + c[1]);
        }
    }

    @Test
    void duckdbVectors() throws java.io.IOException {
        JsonArray cases;
        try (InputStream in = SecretsScopeMatchTest.class.getResourceAsStream(
                "/secrets/scope_match_vectors.json")) {
            assertNotNull(in, "scope_match_vectors.json is not on the test classpath");
            JsonObject doc = JsonParser.parseString(new String(in.readAllBytes(), StandardCharsets.UTF_8))
                    .getAsJsonObject();
            cases = doc.getAsJsonArray("cases");
        }
        assertTrue(cases.size() >= 3000, "only " + cases.size() + " vectors");
        List<String> mismatches = new ArrayList<>();
        for (JsonElement e : cases) {
            JsonObject c = e.getAsJsonObject();
            String scope = c.get("scope").getAsString();
            String path = c.get("path").getAsString();
            boolean want = c.get("match").getAsBoolean();
            if (Secrets.scopeMatches(scope, path) != want) {
                mismatches.add("scope " + scope + " path " + path + " want " + want);
            }
        }
        assertTrue(mismatches.isEmpty(), mismatches.size() + " mismatches: " + mismatches);
    }

    @Test
    void emptyPrefixIsNotAScope() {
        assertFalse(Secrets.scopeMatches("", "s3://bucket/x"));
    }

    private static Map<String, String> fields(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /**
     * An authority-only scope covers its own host and paths under it, never a
     * longer host name or another port.
     */
    @Test
    void selectorRespectsAuthorityBoundary() {
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        byName.put("host1", fields("type", "snmp", "community", "host1", "scope", "snmp://10.0.0.1"));
        byName.put("host15", fields("type", "snmp", "community", "host15", "scope", "snmp://10.0.0.15"));
        byName.put("default", fields("type", "snmp", "community", "default"));
        Secrets s = Secrets.of(byName);
        Map<String, String> want = new LinkedHashMap<>();
        want.put("snmp://10.0.0.1/sys", "host1");
        want.put("snmp://10.0.0.15", "host15");
        want.put("snmp://10.0.0.1:161", "default");
        for (Map.Entry<String, String> w : want.entrySet()) {
            assertEquals(w.getValue(),
                    s.forScopeOfType(w.getKey(), "snmp").map(m -> m.get("community")).orElse(null),
                    "forScopeOfType " + w.getKey());
            assertEquals(w.getValue(), s.fieldFor(w.getKey(), "community").orElse(null),
                    "fieldFor " + w.getKey());
        }
    }
}
