// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import java.util.Arrays;
import java.util.HexFormat;

/**
 * Derives the Cloudflare Durable Object routing key for an attach.
 *
 * <p>The attach id is laid out {@code uuid(16) || ...} ({@code catalog_attach}
 * mints a fresh 16-byte UUID at the head). The shard key is
 * {@code "att-" + hex(uuid)} — one DO per logical ATTACH, stable across
 * re-seals and globally unique (unlike the random-nonce ciphertext or the
 * catalog options bytes). Mirrors vgi-python's {@code _derive_shard_key} and
 * the Go/TypeScript {@code deriveShardKey}.
 */
public final class ShardKey {

    /** Width of the framework UUID at the head of every attach id. */
    public static final int ATTACH_UUID_LEN = 16;

    private static final HexFormat HEX = HexFormat.of();

    private ShardKey() {}

    /**
     * Derives the shard key for an attach id.
     *
     * @param attachId the raw attach id; only its leading 16-byte UUID is used
     * @return {@code "att-" + hex(attachId[0..15])}
     * @throws IllegalArgumentException if {@code attachId} is null or shorter
     *     than 16 bytes — the storage path is always bound to a logical ATTACH.
     */
    public static String derive(byte[] attachId) {
        if (attachId == null || attachId.length < ATTACH_UUID_LEN) {
            throw new IllegalArgumentException(
                    "shard_key requires a 16-byte attach uuid, got "
                            + (attachId == null ? 0 : attachId.length) + " bytes");
        }
        return "att-" + HEX.formatHex(Arrays.copyOfRange(attachId, 0, ATTACH_UUID_LEN));
    }
}
