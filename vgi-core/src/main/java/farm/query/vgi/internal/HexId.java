// Copyright 2025-2026 Query.Farm LLC

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

    /** Hex-encode an arbitrary byte array. Returns "" for null. */
    public static String encode(byte[] bytes) {
        return bytes == null ? "" : HEX.formatHex(bytes);
    }

    /** 16-byte execution id derived from a fresh random UUID. */
    public static byte[] newExecutionId() {
        UUID u = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(u.getMostSignificantBits());
        bb.putLong(u.getLeastSignificantBits());
        return bb.array();
    }

    /** 16 cryptographically random bytes — for attach identifiers. */
    public static byte[] randomBytes(SecureRandom rng, int len) {
        byte[] out = new byte[len];
        rng.nextBytes(out);
        return out;
    }
}
