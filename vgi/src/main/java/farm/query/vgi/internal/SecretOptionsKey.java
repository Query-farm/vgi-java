// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * The AEAD key secret ATTACH option values are encrypted with:
 * {@code HKDF-SHA256(ikm = root, salt = empty, info = "vgi.attach_secret_options.v1", L = 32)}.
 * The root (configured secret-options key, the HTTP signing key, or {@link HostLocalKey}) is
 * never used as the AEAD key itself.
 */
public final class SecretOptionsKey {

    /** HKDF {@code info}, and the AAD prefix of the encrypted blob. */
    public static final String INFO = "vgi.attach_secret_options.v1";

    private SecretOptionsKey() {}

    /**
     * Derive the 32-byte AEAD key from a root key.
     *
     * @param root the root key (any length, normally 32 bytes)
     * @return the derived key
     */
    public static byte[] derive(byte[] root) {
        return hkdf(root, INFO.getBytes(StandardCharsets.UTF_8));
    }

    /** HKDF-SHA256 (RFC 5869) with an empty salt and a 32-byte output: one expand block. */
    static byte[] hkdf(byte[] ikm, byte[] info) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            // Extract: an empty salt is HashLen zero bytes (RFC 5869 2.2).
            mac.init(new SecretKeySpec(new byte[32], "HmacSHA256"));
            byte[] prk = mac.doFinal(ikm);
            // Expand: T(1) = HMAC(PRK, info || 0x01).
            mac.init(new SecretKeySpec(prk, "HmacSHA256"));
            mac.update(info);
            mac.update((byte) 1);
            return mac.doFinal();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }
}
