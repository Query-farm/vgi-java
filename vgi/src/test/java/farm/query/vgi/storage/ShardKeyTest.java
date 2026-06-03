// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import org.junit.jupiter.api.Test;

class ShardKeyTest {

    @Test
    void deriveIsAttPlusHexOfLeading16Bytes() {
        byte[] uuid = new byte[16];
        java.util.Arrays.fill(uuid, (byte) 0xab);
        assertEquals("att-" + "ab".repeat(16), ShardKey.derive(uuid));
        assertTrue(ShardKey.derive(uuid).matches("^att-[0-9a-f]{32}$"));
    }

    @Test
    void usesOnlyTheLeading16BytesOfAFullAttachId() {
        // attach id is uuid(16) || 0x00 || ipc(options) — shard on the uuid only.
        SecureRandom rng = new SecureRandom();
        byte[] uuid = new byte[16];
        rng.nextBytes(uuid);
        byte[] withOptions = new byte[64];
        System.arraycopy(uuid, 0, withOptions, 0, 16);
        rng.nextBytes(java.util.Arrays.copyOfRange(withOptions, 16, 64)); // trailing bytes ignored
        assertEquals(ShardKey.derive(uuid), ShardKey.derive(withOptions));
    }

    @Test
    void distinctUuidsGiveDistinctKeys() {
        SecureRandom rng = new SecureRandom();
        byte[] a = new byte[16];
        byte[] b = new byte[16];
        rng.nextBytes(a);
        rng.nextBytes(b);
        assertNotEquals(ShardKey.derive(a), ShardKey.derive(b));
    }

    @Test
    void rejectsMissingOrShortUuid() {
        assertThrows(IllegalArgumentException.class, () -> ShardKey.derive(null));
        assertThrows(IllegalArgumentException.class, () -> ShardKey.derive(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> ShardKey.derive(new byte[8]));
    }
}
