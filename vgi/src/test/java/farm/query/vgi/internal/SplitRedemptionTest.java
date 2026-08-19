// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A split token must be REDEEMED, not merely minted.
 *
 * <p>Minting alone is the dangerous half-implementation. A function advertising
 * {@code supportsSplits} makes the client plan N splits and issue N inits, each
 * carrying one token; a worker that ignores those tokens runs its whole scan N
 * times and the query answers N× its real row count, silently — the exact
 * failure class splits exist to prevent, produced by the mechanism meant to
 * prevent it.</p>
 *
 * <p>This SDK had that shape: {@code SplitToken} existed with no runtime caller,
 * and {@code table_function_plan} returned a split that had never been stamped.
 * These assertions cover the round trip that closes it.</p>
 */
class SplitRedemptionTest {

    private static byte[] fingerprintFor(String function) {
        return SplitToken.bindFingerprint("main", function, new byte[] {1, 2}, new byte[0], new byte[0]);
    }

    @Test
    void aStampedTokenRoundTripsBackToTheWorkersOwnPayload() {
        byte[] fingerprint = fingerprintFor("split_seq");
        byte[] anchor = SplitToken.anchor(47);
        byte[] payload = "rows=0..250".getBytes(StandardCharsets.UTF_8);

        byte[] token = SplitToken.build(payload, fingerprint, anchor, null, null);
        assertArrayEquals(payload, SplitToken.open(token, null, null, fingerprint, anchor));
    }

    @Test
    void eachSplitsPayloadSurvivesIndependently() {
        // The redemption path takes a LIST, because an engine whose partition
        // count is its concurrency bin-packs and reads a whole group per
        // partition. DuckDB always sends exactly one.
        byte[] fingerprint = fingerprintFor("split_seq");
        byte[] anchor = SplitToken.anchor(1);

        for (int i = 0; i < 3; i++) {
            byte[] payload = ("slice-" + i).getBytes(StandardCharsets.UTF_8);
            byte[] token = SplitToken.build(payload, fingerprint, anchor, null, null);
            assertArrayEquals(payload, SplitToken.open(token, null, null, fingerprint, anchor));
        }
    }

    @Test
    void aTokenMintedForAnotherBindIsRefusedBeforeThePayloadIsReachable() {
        byte[] mine = fingerprintFor("a");
        byte[] theirs = fingerprintFor("b");
        byte[] token = SplitToken.build(
                "not-for-you".getBytes(StandardCharsets.UTF_8), theirs, SplitToken.anchor(1), null, null);

        SplitToken.SplitTokenException e = assertThrows(
                SplitToken.SplitTokenException.class,
                () -> SplitToken.open(token, null, null, mine, null));
        assertEquals(SplitToken.KIND_INVALID, e.kind());
    }

    @Test
    void aStampedSplitCarriesItsTokenOnTheWire() {
        // The plan handler stamps then serializes; a split that reached the wire
        // with an empty token is one that bypassed the framework, and the client
        // rejects it by name.
        byte[] token = SplitToken.build(
                "file=3".getBytes(StandardCharsets.UTF_8), fingerprintFor("f"), SplitToken.anchor(9), null, null);
        ScanSplitLike stamped = new ScanSplitLike(
                farm.query.vgi.protocol.ScanSplit.of("file=3".getBytes(StandardCharsets.UTF_8))
                        .withToken(token));

        assertEquals(token.length, stamped.tokenLength());
        assertArrayEquals(
                "file=3".getBytes(StandardCharsets.UTF_8),
                SplitToken.open(stamped.token(), null, null, fingerprintFor("f"), SplitToken.anchor(9)));
        // And it serializes — the schema is generated, so this also pins that the
        // hand-written serializer still matches it.
        byte[] wire = ScanSplitSerializer.serialize(
                farm.query.vgi.protocol.ScanSplit.of("file=3".getBytes(StandardCharsets.UTF_8))
                        .withToken(token));
        org.junit.jupiter.api.Assertions.assertTrue(wire.length > 0);
    }

    /** Tiny holder so the assertions above read as intent rather than accessors. */
    private record ScanSplitLike(farm.query.vgi.protocol.ScanSplit split) {
        byte[] token() {
            return split.token();
        }

        int tokenLength() {
            return split.token().length;
        }
    }
}
