// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgi.AttachOptions;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Secret ATTACH options encrypted into the attach id, and the host-local key. */
class AttachSecretOptionsTest {

    static final List<AttachOptionSpec> SPECS = List.of(
            AttachOptionSpec.of("region", "plain", Schemas.UTF8, "us-east-1"),
            AttachOptionSpec.of("token", "secret", Schemas.UTF8, null).asSecret());

    static byte[] userOptions(String token) {
        Schema s = new Schema(List.of(new Field("token", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(s, Allocators.root())) {
            root.allocateNew();
            ((VarCharVector) root.getVector(0)).setSafe(0, token.getBytes(StandardCharsets.UTF_8));
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    @Test
    void secretsAreEncryptedAndOpenOnlyWithTheKey() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        byte[] id = AttachOptionsAttachId.encode(SPECS, userOptions("hunter2-secret"), new SecureRandom(), key);
        assertFalse(new String(id, StandardCharsets.ISO_8859_1).contains("hunter2-secret"));
        assertTrue(AttachOptionsAttachId.hasSecrets(id));
        try (VectorSchemaRoot plain = AttachOptionsAttachId.decode(id, Allocators.root())) {
            assertEquals(List.of("region"), plain.getSchema().getFields().stream().map(Field::getName).toList(),
                    "the plain batch keeps its old shape");
        }
        try (VectorSchemaRoot secrets = AttachOptionsAttachId.openSecrets(id, key, Allocators.root())) {
            assertEquals("hunter2-secret", secrets.getVector("token").getObject(0).toString());
        }
        assertThrows(IllegalArgumentException.class,
                () -> AttachOptionsAttachId.openSecrets(id, new byte[32], Allocators.root()));
        // Moved under another attach's uuid, it does not open (the AAD binds it).
        byte[] moved = id.clone();
        moved[0] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> AttachOptionsAttachId.openSecrets(moved, key, Allocators.root()));
    }

    /** The AEAD key is HKDF-SHA256(root, info "vgi.attach_secret_options.v1"), never the root. */
    @Test
    void theBlobIsVersionedAndKeyedByTheHkdfSubkey() throws Exception {
        byte[] root = new byte[32];
        new SecureRandom().nextBytes(root);
        byte[] id = AttachOptionsAttachId.encode(SPECS, userOptions("hunter2-secret"), new SecureRandom(), root);
        String b64;
        try (org.apache.arrow.vector.ipc.ArrowStreamReader r = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                new java.io.ByteArrayInputStream(id, 17, id.length - 17), Allocators.root())) {
            b64 = r.getVectorSchemaRoot().getSchema().getCustomMetadata().get(AttachOptionsAttachId.SECRET_OPTIONS_KEY);
        }
        byte[] blob = java.util.Base64.getDecoder().decode(b64);
        assertEquals(1, blob[0], "version byte");
        byte[] body = java.util.Arrays.copyOfRange(blob, 1, blob.length);
        byte[] aad = new byte[SecretOptionsKey.INFO.length() + 1 + 16];
        System.arraycopy(SecretOptionsKey.INFO.getBytes(StandardCharsets.UTF_8), 0, aad, 0, SecretOptionsKey.INFO.length());
        System.arraycopy(id, 0, aad, SecretOptionsKey.INFO.length() + 1, 16);
        assertThrows(RuntimeException.class,
                () -> farm.query.vgirpc.http.auth.Crypto.chacha20Poly1305Open(root, body, aad), "the root key does not open it");
        byte[] ipc = farm.query.vgirpc.http.auth.Crypto.chacha20Poly1305Open(SecretOptionsKey.derive(root), body, aad);
        try (VectorSchemaRoot r = BatchUtil.readSingleBatch(ipc, Allocators.root())) {
            assertEquals("hunter2-secret", r.getVector("token").getObject(0).toString());
        }
        // RFC 5869 test case 3 (empty salt and info): the first 32 bytes of the OKM.
        byte[] ikm = new byte[22];
        java.util.Arrays.fill(ikm, (byte) 0x0b);
        assertEquals("8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d",
                java.util.HexFormat.of().formatHex(SecretOptionsKey.hkdf(ikm, new byte[0])));
        assertEquals(32, SecretOptionsKey.derive(root).length);
        assertFalse(java.util.Arrays.equals(root, SecretOptionsKey.derive(root)));
    }

    @Test
    void withoutASecretValueTheFormatIsUnchangedAndNoKeyIsNeeded() {
        byte[] id = AttachOptionsAttachId.encode(SPECS, null, new SecureRandom(),
                (java.util.function.Supplier<byte[]>) () -> { throw new AssertionError("no key needed"); });
        assertFalse(AttachOptionsAttachId.hasSecrets(id));
        assertEquals("us-east-1", AttachOptions.fromAttachId(id).getString("region"));
        assertTrue(AttachOptions.fromAttachId(id).secretsAvailable());
        assertNull(AttachOptions.fromAttachId(new byte[16]).get("region"), "a plain uuid carries no options");
    }

    @Test
    void theLegacyEncodeStillDropsSecrets() {
        byte[] id = AttachOptionsAttachId.encode(SPECS, userOptions("hunter2-secret"), new SecureRandom());
        assertFalse(AttachOptionsAttachId.hasSecrets(id));
        assertFalse(new String(id, StandardCharsets.ISO_8859_1).contains("hunter2-secret"));
    }

    @Test
    void hostLocalKeyIsCreatedPrivatelyOnceAndReused(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("sub").resolve("k.key");
        byte[] first = HostLocalKey.load(file);
        assertEquals(32, first.length);
        assertArrayEquals(first, HostLocalKey.load(file), "a second process reads the same key");
        if (Files.getFileStore(file).supportsFileAttributeView("posix")) {
            assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
            assertEquals("rwx------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file.getParent())));
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));
            assertThrows(IllegalStateException.class, () -> HostLocalKey.load(file), "a readable key is refused");
        }
    }

    @Test
    void concurrentFirstStartsAgreeOnOneKey(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("race").resolve("k.key");
        int n = 16;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(n);
        try {
            java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
            List<java.util.concurrent.Future<byte[]>> keys = new java.util.ArrayList<>();
            for (int i = 0; i < n; i++) keys.add(pool.submit(() -> { go.await(); return HostLocalKey.load(file); }));
            go.countDown();
            byte[] first = keys.get(0).get();
            for (var k : keys) assertArrayEquals(first, k.get());
        } finally {
            pool.shutdownNow();
        }
        try (var listing = Files.list(file.getParent())) {
            assertEquals(List.of("k.key"), listing.map(p -> p.getFileName().toString()).toList(), "no temp files left");
        }
    }

    @Test
    void aKeyFileOfTheWrongSizeIsRefused(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("k.key");
        Files.write(file, new byte[16]);
        if (Files.getFileStore(file).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        }
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> HostLocalKey.load(file));
        assertTrue(e.getMessage().contains(file.toString()) && e.getMessage().contains("VGI_SIGNING_KEY"), e.getMessage());
    }
}
