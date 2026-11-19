// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import farm.query.vgirpc.vendor.BlobRef;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The v1 storage envelope, byte for byte against the vgi extension's
 * {@code test/vectors/storage_envelope_v1.json} (a copy is a test resource; when
 * a vgi checkout is found through {@code VGI_DIR} or the sibling {@code ../vgi},
 * the copy must equal it).
 */
class StorageEnvelopeVectorsTest {

    private static final HexFormat HEX = HexFormat.of();

    private static byte[] resourceBytes() throws Exception {
        try (InputStream in = StorageEnvelopeVectorsTest.class.getResourceAsStream("/storage_envelope_v1.json")) {
            return in.readAllBytes();
        }
    }

    private static JsonObject vectors() throws Exception {
        return JsonParser.parseString(new String(resourceBytes(), StandardCharsets.UTF_8)).getAsJsonObject();
    }

    @Test
    void constants() throws Exception {
        JsonObject v = vectors();
        assertEquals("vgi.storage_envelope.v1", v.get("format").getAsString());
        assertEquals(StorageEnvelope.TAG_INLINE, v.get("tag_inline").getAsInt());
        assertEquals(StorageEnvelope.TAG_POINTER, v.get("tag_pointer").getAsInt());
        assertEquals(StorageEnvelope.POINTER_FIXED, v.get("pointer_fixed_bytes").getAsInt());
        assertEquals(ExternalizingStorage.DEFAULT_EXTERNALIZE_BYTES, v.get("default_externalize_bytes").getAsLong());
    }

    @Test
    void inline() throws Exception {
        for (JsonElement e : vectors().getAsJsonArray("inline")) {
            JsonObject c = e.getAsJsonObject();
            byte[] value = HEX.parseHex(c.get("value_hex").getAsString());
            byte[] envelope = HEX.parseHex(c.get("envelope_hex").getAsString());
            String name = c.get("name").getAsString();
            assertArrayEquals(envelope, StorageEnvelope.encodeInline(value), name);
            assertEquals(StorageEnvelope.TAG_INLINE, StorageEnvelope.tag(envelope), name);
            assertArrayEquals(value, StorageEnvelope.inlineValue(envelope), name);
        }
    }

    @Test
    void pointers() throws Exception {
        for (JsonElement e : vectors().getAsJsonArray("pointers")) {
            JsonObject c = e.getAsJsonObject();
            BlobRef ref = new BlobRef(c.get("object_id").getAsString(), c.get("size").getAsLong(),
                    c.get("sha256").getAsString());
            byte[] envelope = HEX.parseHex(c.get("envelope_hex").getAsString());
            String name = c.get("name").getAsString();
            assertArrayEquals(envelope, StorageEnvelope.encodePointer(ref), name);
            assertEquals(StorageEnvelope.TAG_POINTER, StorageEnvelope.tag(envelope), name);
            assertEquals(ref, StorageEnvelope.decodePointer(envelope), name);
        }
    }

    @Test
    void malformed() throws Exception {
        for (JsonElement e : vectors().getAsJsonArray("malformed")) {
            JsonObject c = e.getAsJsonObject();
            byte[] envelope = HEX.parseHex(c.get("envelope_hex").getAsString());
            String name = c.get("name").getAsString();
            String kind = c.get("error").getAsString();
            StorageEnvelopeException ex = assertThrows(StorageEnvelopeException.class, () -> {
                if (StorageEnvelope.tag(envelope) == StorageEnvelope.TAG_POINTER) {
                    StorageEnvelope.decodePointer(envelope);
                }
            }, name);
            String expected = switch (kind) {
                case "empty" -> "empty stored value";
                case "unknown_tag" -> "unknown envelope tag";
                case "malformed_pointer" -> "malformed blob pointer";
                default -> throw new AssertionError("unknown error kind " + kind);
            };
            assertEquals(true, ex.getMessage().contains(expected), name + ": " + ex.getMessage());
        }
    }

    @Test
    void scopeExecutionIds() throws Exception {
        for (JsonElement e : vectors().getAsJsonObject("scope_execution_ids").getAsJsonArray("cases")) {
            JsonObject c = e.getAsJsonObject();
            assertEquals(c.get("execution_id").getAsString(), StorageEnvelope.blobExecutionId(
                    c.get("kind").getAsString(), HEX.parseHex(c.get("ident_hex").getAsString())));
        }
    }

    @Test
    void scopeShards() throws Exception {
        for (JsonElement e : vectors().getAsJsonObject("scope_shards").getAsJsonArray("cases")) {
            JsonObject c = e.getAsJsonObject();
            assertEquals(c.get("shard").getAsString(), StorageEnvelope.blobShard(c.get("shard_key").getAsString()));
        }
    }

    @Test
    void copyMatchesTheVgiCheckout() throws Exception {
        String dir = System.getenv("VGI_DIR");
        Path root = dir != null && !dir.isBlank() ? Path.of(dir) : Path.of("..", "..", "vgi");
        Path upstream = root.resolve("test/vectors/storage_envelope_v1.json");
        assumeTrue(Files.isRegularFile(upstream), "no vgi checkout with test/vectors at " + root);
        assertArrayEquals(Files.readAllBytes(upstream), resourceBytes(),
                "vgi-java's storage_envelope_v1.json copy differs from " + upstream);
    }
}
