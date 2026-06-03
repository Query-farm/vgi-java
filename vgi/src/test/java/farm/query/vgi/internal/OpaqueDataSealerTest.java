// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.AuthContext;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpaqueDataSealerTest {

    private static final byte[] PLAIN = "attach-secret".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TXN = "txn-secret".getBytes(StandardCharsets.UTF_8);
    private static final AuthContext ALICE =
            new AuthContext("acme", true, "alice", Map.of());
    private static final AuthContext BOB =
            new AuthContext("acme", true, "bob", Map.of());

    @Test
    void disabledSealerIsPurePassthrough() {
        OpaqueDataSealer s = new OpaqueDataSealer(false);
        assertFalse(s.enabled());
        assertArrayEquals(PLAIN, s.sealAttach(PLAIN, ALICE));
        assertArrayEquals(PLAIN, s.unsealAttach(PLAIN, ALICE));
        assertArrayEquals(TXN, s.sealTransaction(TXN, PLAIN, ALICE));
        assertArrayEquals(TXN, s.unsealTransaction(TXN, PLAIN, ALICE));
    }

    @Test
    void attachRoundTrips() {
        OpaqueDataSealer s = new OpaqueDataSealer(true);
        assertTrue(s.enabled());
        byte[] sealed = s.sealAttach(PLAIN, ALICE);
        assertFalse(java.util.Arrays.equals(PLAIN, sealed));
        assertArrayEquals(PLAIN, s.unsealAttach(sealed, ALICE));
    }

    @Test
    void transactionRoundTripsBoundToParentAttach() {
        OpaqueDataSealer s = new OpaqueDataSealer(true);
        byte[] attachEnvelope = s.sealAttach(PLAIN, ALICE);
        byte[] sealedTxn = s.sealTransaction(TXN, attachEnvelope, ALICE);
        assertArrayEquals(TXN, s.unsealTransaction(sealedTxn, attachEnvelope, ALICE));
    }

    @Test
    void crossPrincipalUnsealIsRejected() {
        OpaqueDataSealer s = new OpaqueDataSealer(true);
        byte[] sealed = s.sealAttach(PLAIN, ALICE);
        assertThrows(IllegalArgumentException.class, () -> s.unsealAttach(sealed, BOB));
    }

    @Test
    void transactionUnsealRejectsWrongParentAttach() {
        OpaqueDataSealer s = new OpaqueDataSealer(true);
        byte[] attachA = s.sealAttach(PLAIN, ALICE);
        byte[] attachB = s.sealAttach("other".getBytes(StandardCharsets.UTF_8), ALICE);
        byte[] sealedTxn = s.sealTransaction(TXN, attachA, ALICE);
        assertThrows(IllegalArgumentException.class,
                () -> s.unsealTransaction(sealedTxn, attachB, ALICE));
    }

    @Test
    void tamperedEnvelopeIsRejected() {
        OpaqueDataSealer s = new OpaqueDataSealer(true);
        byte[] sealed = s.sealAttach(PLAIN, ALICE);
        sealed[sealed.length - 1] ^= 0x01;
        assertThrows(IllegalArgumentException.class, () -> s.unsealAttach(sealed, ALICE));
    }
}
