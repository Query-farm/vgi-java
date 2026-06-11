// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Helpers for the 16-byte execution / attach identifiers used throughout the
 * VGI protocol. The byte payload is wire-portable; the hex form is the
 * canonical Map key (workers store per-execution state in
 * {@code ConcurrentHashMap<String, ...>}).
 */
public final class HexId {

    private static final HexFormat HEX = HexFormat.of();

    private HexId() {}

    /**
     * Hex-encode an arbitrary byte array.
     *
     * @param bytes the bytes to encode, or {@code null}
     * @return the lower-case hex string, or {@code ""} for {@code null}
     */
    public static String encode(byte[] bytes) {
        return bytes == null ? "" : HEX.formatHex(bytes);
    }

    /**
     * Mint a fresh execution identifier.
     *
     * @return a 16-byte execution id derived from a fresh random UUID
     */
    public static byte[] newExecutionId() {
        UUID u = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(u.getMostSignificantBits());
        bb.putLong(u.getLeastSignificantBits());
        return bb.array();
    }

    /**
     * Cryptographically random bytes — for attach identifiers.
     *
     * @param rng source of randomness
     * @param len number of bytes to generate
     * @return a fresh array of {@code len} random bytes
     */
    public static byte[] randomBytes(SecureRandom rng, int len) {
        byte[] out = new byte[len];
        rng.nextBytes(out);
        return out;
    }
}
