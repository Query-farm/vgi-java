// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    // --- vgi-secrets.md section 5 selection: by type is by the `type` field. ---

    /** The client keys columns by secret NAME; an unnamed secret is __default_<type>. */
    private static Secrets nameKeyed() {
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        byName.put("__default_brave", fields("type", "brave", "name", "__default_brave", "api_key", "k1", "scope", ""));
        return Secrets.of(byName);
    }

    @Test
    void byTypeFindsUnnamedDefaultSecret() {
        Secrets s = nameKeyed();
        assertEquals("k1", s.lookup("brave").orElseThrow().get("api_key"));
        assertEquals("k1", s.require("brave").get("api_key"));
        assertEquals(1, s.ofType("brave").size());
        // The column name is not the type: a column-name lookup would miss it.
        assertTrue(s.namedField("brave", "api_key").isEmpty());
    }

    @Test
    void byTypeFindsSecretNamedUnlikeItsType() {
        Secrets s = Secrets.of(Map.of("conformance_creds",
                fields("type", "vgi_example", "api_key", "named-key")));
        assertEquals("named-key", s.lookup("vgi_example").orElseThrow().get("api_key"));
        assertTrue(s.lookup("conformance_creds").isEmpty());
    }

    @Test
    void byTypeFallsBackToColumnNameOnlyWithoutTypeField() {
        Secrets legacy = Secrets.of(Map.of("brave", fields("api_key", "legacy")));
        assertEquals("legacy", legacy.lookup("brave").orElseThrow().get("api_key"));
        assertEquals(1, legacy.ofType("brave").size());
        // A secret that does carry a type field is never matched by its column name.
        Secrets typed = Secrets.of(Map.of("brave", fields("type", "s3", "api_key", "x")));
        assertTrue(typed.lookup("brave").isEmpty());
    }

    @Test
    void byTypePrefersUnscopedThenFirst() {
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        byName.put("a_scoped", fields("type", "t", "v", "scoped", "scope", "x://a/"));
        byName.put("b_unscoped", fields("type", "t", "v", "unscoped", "scope", ""));
        assertEquals("unscoped", Secrets.of(byName).lookup("t").orElseThrow().get("v"));
        byName.remove("b_unscoped");
        assertEquals("scoped", Secrets.of(byName).lookup("t").orElseThrow().get("v"));
        assertTrue(Secrets.of(byName).lookup("other").isEmpty());
    }

    @Test
    void requireNamesTheTypeAndNoValue() {
        Secrets s = Secrets.of(Map.of("other", fields("type", "s3", "secret", "sk-do-not-leak")));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> s.require("vgi_example"));
        assertTrue(e.getMessage().contains("vgi_example"), e.getMessage());
        assertFalse(e.getMessage().contains("sk-do-not-leak"), e.getMessage());
    }

    @Test
    void byNameChecksType() {
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        byName.put("one", fields("type", "vgi_example", "api_key", "key-one"));
        byName.put("two", fields("type", "vgi_example", "api_key", "key-two"));
        byName.put("creds_s3", fields("type", "s3", "key_id", "AK"));
        Secrets s = Secrets.of(byName);
        assertEquals("key-one", s.lookupNamed("vgi_example", "one").orElseThrow().get("api_key"));
        assertEquals("key-two", s.lookupNamed("vgi_example", "two").orElseThrow().get("api_key"));
        assertTrue(s.lookupNamed("vgi_example", "creds_s3").isEmpty());
        assertTrue(s.lookupNamed("vgi_example", "missing").isEmpty());
    }

    @Test
    void sameTypeNestedScopes() {
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        byName.put("conformance_a", fields("type", "vgi_example", "v", "a", "scope", "vgi-conformance://a/"));
        byName.put("conformance_ab", fields("type", "vgi_example", "v", "ab", "scope", "vgi-conformance://a/b/"));
        byName.put("conformance_any", fields("type", "vgi_example", "v", "any", "scope", ""));
        byName.put("foreign", fields("type", "s3", "v", "s3", "scope", "vgi-conformance://a/b/c/"));
        Secrets s = Secrets.of(byName);
        String t = "vgi_example";
        assertEquals("ab", s.forScopeOfType("vgi-conformance://a/b/c/x", t).orElseThrow().get("v"));
        assertEquals("a", s.forScopeOfType("vgi-conformance://a/x", t).orElseThrow().get("v"));
        assertEquals("any", s.forScopeOfType("vgi-conformance://z/x", t).orElseThrow().get("v"));
        byName.remove("conformance_any");
        assertTrue(Secrets.of(byName).forScopeOfType("vgi-conformance://z/x", t).isEmpty());
        byName.remove("conformance_ab");
        assertEquals("a", Secrets.of(byName).forScopeOfType("vgi-conformance://a/b/x", t).orElseThrow().get("v"));
    }

    @Test
    void parseKeysByColumnAndMatchesByTypeField() throws Exception {
        org.apache.arrow.memory.BufferAllocator alloc = farm.query.vgirpc.wire.Allocators.root();
        org.apache.arrow.vector.types.pojo.FieldType utf8 =
                org.apache.arrow.vector.types.pojo.FieldType.nullable(
                        org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE);
        java.util.List<org.apache.arrow.vector.types.pojo.Field> children = java.util.List.of(
                new org.apache.arrow.vector.types.pojo.Field("type", utf8, null),
                new org.apache.arrow.vector.types.pojo.Field("api_key", utf8, null),
                new org.apache.arrow.vector.types.pojo.Field("scope", utf8, null));
        org.apache.arrow.vector.types.pojo.Field col = new org.apache.arrow.vector.types.pojo.Field(
                "__default_vgi_example",
                org.apache.arrow.vector.types.pojo.FieldType.nullable(
                        org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE), children);
        byte[] bytes;
        try (org.apache.arrow.vector.VectorSchemaRoot root = org.apache.arrow.vector.VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(col)), alloc)) {
            root.allocateNew();
            org.apache.arrow.vector.complex.StructVector sv =
                    (org.apache.arrow.vector.complex.StructVector) root.getVector(0);
            sv.setIndexDefined(0);
            ((org.apache.arrow.vector.VarCharVector) sv.getChild("type")).setSafe(0, "vgi_example".getBytes());
            ((org.apache.arrow.vector.VarCharVector) sv.getChild("api_key")).setSafe(0, "k".getBytes());
            ((org.apache.arrow.vector.VarCharVector) sv.getChild("scope")).setSafe(0, new byte[0]);
            root.setRowCount(1);
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            try (org.apache.arrow.vector.ipc.ArrowStreamWriter w =
                         new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, out)) {
                w.start();
                w.writeBatch();
                w.end();
            }
            bytes = out.toByteArray();
        }
        Secrets s = Secrets.parse(bytes);
        Map<String, String> got = s.lookup("vgi_example").orElseThrow();
        assertEquals("k", got.get("api_key"));
        assertEquals("__default_vgi_example", got.get("name"));
        assertTrue(s.byName().containsKey("__default_vgi_example"));
    }
}
