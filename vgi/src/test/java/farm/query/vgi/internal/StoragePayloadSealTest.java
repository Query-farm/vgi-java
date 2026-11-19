// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import farm.query.vgi.internal.StoragePayloadSeal.StoragePayloadException;
import farm.query.vgi.storage.BoundStorage;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/** Sealed framework payload v1: round trip, tamper, moved slot, wrong key and the cross-SDK vector. */
class StoragePayloadSealTest {

    private static final HexFormat HEX = HexFormat.of();
    private static final byte[] KEY_A = "k".repeat(43).getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY_B = "z".repeat(43).getBytes(StandardCharsets.UTF_8);
    private static final String SHARD = "att-0123";
    private static final byte[] SCOPE = filled(16, 1);
    private static final byte[] NS = "_vgi/buffering_init".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY = "k".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PLAIN = "secret context".getBytes(StandardCharsets.UTF_8);

    private static byte[] filled(int n, int v) {
        byte[] b = new byte[n];
        java.util.Arrays.fill(b, (byte) v);
        return b;
    }

    @Test
    void roundTrip() {
        StoragePayloadSeal s = StoragePayloadSeal.forKey(KEY_A);
        byte[] sealed = s.seal(PLAIN, SHARD, SCOPE, NS, KEY);
        assertEquals(StoragePayloadSeal.VERSION, sealed[0]);
        assertEquals(1 + 12 + PLAIN.length + 16, sealed.length);
        assertFalse(new String(sealed, StandardCharsets.ISO_8859_1).contains("secret context"));
        assertArrayEquals(PLAIN, s.open(sealed, SHARD, SCOPE, NS, KEY));
        // Random nonce: two seals of the same payload differ.
        assertFalse(java.util.Arrays.equals(sealed, s.seal(PLAIN, SHARD, SCOPE, NS, KEY)));
    }

    @Test
    void tamperedRefused() {
        StoragePayloadSeal s = StoragePayloadSeal.forKey(KEY_A);
        byte[] sealed = s.seal(PLAIN, SHARD, SCOPE, NS, KEY);
        for (int i : new int[] {1, 13, sealed.length - 1}) {
            byte[] t = sealed.clone();
            t[i] ^= 1;
            StoragePayloadException e = assertThrows(StoragePayloadException.class,
                    () -> s.open(t, SHARD, SCOPE, NS, KEY));
            assertTrue(e.getMessage().contains("failed authentication"), e.getMessage());
        }
    }

    @Test
    void movedPayloadRefused() {
        StoragePayloadSeal s = StoragePayloadSeal.forKey(KEY_A);
        byte[] sealed = s.seal(PLAIN, SHARD, SCOPE, NS, KEY);
        assertThrows(StoragePayloadException.class, () -> s.open(sealed, "att-9999", SCOPE, NS, KEY));
        assertThrows(StoragePayloadException.class, () -> s.open(sealed, SHARD, filled(16, 2), NS, KEY));
        assertThrows(StoragePayloadException.class,
                () -> s.open(sealed, SHARD, SCOPE, "_vgi/aggregate_state".getBytes(StandardCharsets.UTF_8), KEY));
        assertThrows(StoragePayloadException.class,
                () -> s.open(sealed, SHARD, SCOPE, NS, "other".getBytes(StandardCharsets.UTF_8)));
        assertThrows(StoragePayloadException.class, () -> s.open(sealed, "", SCOPE, NS, KEY));
    }

    @Test
    void wrongKeyRefused() {
        byte[] sealed = StoragePayloadSeal.forKey(KEY_A).seal(PLAIN, SHARD, SCOPE, NS, KEY);
        assertThrows(StoragePayloadException.class,
                () -> StoragePayloadSeal.forKey(KEY_B).open(sealed, SHARD, SCOPE, NS, KEY));
    }

    @Test
    void unknownVersionShortAndPlaintextRefused() {
        StoragePayloadSeal s = StoragePayloadSeal.forKey(KEY_A);
        byte[] sealed = s.seal(PLAIN, SHARD, SCOPE, NS, KEY);
        byte[] v2 = sealed.clone();
        v2[0] = 0x02;
        StoragePayloadException e = assertThrows(StoragePayloadException.class,
                () -> s.open(v2, SHARD, SCOPE, NS, KEY));
        assertTrue(e.getMessage().contains("unknown seal version"), e.getMessage());
        e = assertThrows(StoragePayloadException.class,
                () -> s.open(java.util.Arrays.copyOf(sealed, 20), SHARD, SCOPE, NS, KEY));
        assertTrue(e.getMessage().contains("too short"), e.getMessage());
        assertThrows(StoragePayloadException.class, () -> s.open(new byte[0], SHARD, SCOPE, NS, KEY));
        // An unsealed value (what 0.48 wrote) is refused, never treated as plaintext.
        byte[] legacy = ("\u0001" + "x".repeat(64)).getBytes(StandardCharsets.UTF_8);
        assertThrows(StoragePayloadException.class, () -> s.open(legacy, SHARD, SCOPE, NS, KEY));
    }

    // Cross-SDK test vector for sealed framework payload v1 (the same bytes are
    // asserted by vgi-python's tests/test_storage_seal.py).
    private static final byte[] V_IKM = "vgi-test-signing-key-0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    private static final String V_SHARD = "att-00112233445566778899aabbccddeeff";
    private static final byte[] V_NS = "_vgi/buffering_init".getBytes(StandardCharsets.UTF_8);
    private static final byte[] V_PLAIN = "vgi sealed framework payload v1".getBytes(StandardCharsets.UTF_8);
    static final String V_DERIVED_KEY_HEX = "f5f759ad2009e91cf4b963a20b524a800224d2ac4e01472396dc1a2dab78bea9";
    static final String V_AAD_HEX = "7667692e73746f726167652e7061796c6f61642e7631000000246174742d30303131323233"
            + "333434353536363737383839396161626263636464656566660000001000010203040506070809"
            + "0a0b0c0d0e0f000000135f7667692f627566666572696e675f696e697400000008ffffffffffffffff";
    static final String V_WIRE_HEX = "01000102030405060708090a0bc79698647483a87eb533cf7332d1c527275cc7470dc6a3"
            + "bdf1f72f624a97e246416d4de55281bd0eee6d6491796a40";

    private static byte[] range(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) i;
        return b;
    }

    @Test
    void crossSdkVector() {
        byte[] key = BoundStorage.packIntKey(-1);
        assertEquals(V_DERIVED_KEY_HEX, HEX.formatHex(StoragePayloadSeal.deriveKey(V_IKM)));
        byte[] aad = StoragePayloadSeal.aad(V_SHARD, range(16), V_NS, key);
        assertEquals(V_AAD_HEX, HEX.formatHex(aad));
        byte[] wire = StoragePayloadSeal.sealWith(StoragePayloadSeal.deriveKey(V_IKM), range(12), V_PLAIN, aad);
        assertEquals(V_WIRE_HEX, HEX.formatHex(wire));
        assertArrayEquals(V_PLAIN,
                StoragePayloadSeal.forKey(V_IKM).open(HEX.parseHex(V_WIRE_HEX), V_SHARD, range(16), V_NS, key));
    }
}
