// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.auth.Crypto;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Seals / unseals {@code attach_opaque_data} and {@code transaction_opaque_data}
 * with ChaCha20-Poly1305 AEAD, AAD-bound to the calling principal. Mirrors
 * vgi-python's {@code Worker._seal_attach} / {@code _seal_transaction}
 * (commit {@code 6a8d97c}).
 *
 * <p><b>HTTP-transport only.</b> Constructed disabled for stdio / AF_UNIX
 * workers — there OS process ownership already enforces identity, so every
 * method is pure passthrough. On HTTP one JVM serves many principals, so the
 * opaque blobs it hands out must not be forgeable or replayable across
 * principals: the AAD binds {@code (domain, principal)}, and a transaction
 * envelope additionally binds its parent attach envelope.
 *
 * <p>Wire format: {@code [version byte][12-byte nonce][ciphertext+tag]}.
 * The key is an ephemeral per-process 32-byte random value — sealed blobs are
 * valid only for the lifetime of the worker process (clients re-ATTACH on
 * restart), and are intentionally not portable across language ports.
 */
public final class OpaqueDataSealer {

    private static final byte VERSION_ATTACH = 1;
    private static final byte VERSION_TRANSACTION = 2;
    private static final byte[] ATTACH_AAD_PREFIX =
            "vgi.attach_opaque_data.v1\0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TRANSACTION_AAD_PREFIX =
            "vgi.transaction_opaque_data.v1\0".getBytes(StandardCharsets.UTF_8);

    /** {@code null} ⇒ disabled (passthrough) — stdio / AF_UNIX transports. */
    private final byte[] key;

    /**
     * Random per-process key. Sealed blobs are only valid for the lifetime
     * of this single worker process; clients must re-ATTACH against any
     * other replica. For multi-replica deployments use
     * {@link #OpaqueDataSealer(byte[])} with a key sourced from a shared
     * secret so blobs decrypt across replicas.
     *
     * @param enabled {@code true} to seal (HTTP transport); {@code false} for passthrough (stdio / AF_UNIX)
     */
    public OpaqueDataSealer(boolean enabled) {
        if (enabled) {
            this.key = new byte[32];
            new SecureRandom().nextBytes(this.key);
        } else {
            this.key = null;
        }
    }

    /**
     * Explicit-key form for multi-replica HTTP deployments. {@code key} must
     * be exactly 32 bytes; {@code null} disables the sealer (same as
     * {@code OpaqueDataSealer(false)}). When set, every replica that
     * receives the same key can unseal blobs the others produced — so a
     * load balancer rotating across replicas does not invalidate the
     * client's {@code attach_opaque_data}.
     *
     * @param key exactly 32 bytes of shared key material, or {@code null} to disable sealing
     */
    public OpaqueDataSealer(byte[] key) {
        if (key == null) {
            this.key = null;
        } else {
            if (key.length != 32) {
                throw new IllegalArgumentException(
                        "OpaqueDataSealer key must be 32 bytes (got " + key.length + ")");
            }
            this.key = key.clone();
        }
    }

    /**
     * Whether this sealer actually seals or merely passes blobs through.
     *
     * @return {@code true} when sealing is active (HTTP transport); {@code false} for passthrough
     */
    public boolean enabled() { return key != null; }

    // --- attach_opaque_data ------------------------------------------------

    /**
     * Seal {@code attach_opaque_data} bound to the calling principal.
     *
     * @param plaintext the worker's plain attach token
     * @param auth the calling principal, bound into the AEAD AAD
     * @return the sealed envelope, or {@code plaintext} unchanged when disabled / {@code null}
     */
    public byte[] sealAttach(byte[] plaintext, AuthContext auth) {
        if (key == null || plaintext == null) return plaintext;
        return envelope(VERSION_ATTACH, Crypto.chacha20Poly1305Seal(key, plaintext, attachAad(auth)));
    }

    /**
     * Unseal an {@code attach_opaque_data} envelope produced by {@link #sealAttach}.
     *
     * @param envelope the sealed envelope received from the client
     * @param auth the calling principal; must match the one that sealed it
     * @return the recovered plain attach token, or {@code envelope} unchanged when disabled / empty
     */
    public byte[] unsealAttach(byte[] envelope, AuthContext auth) {
        if (key == null || envelope == null || envelope.length == 0) return envelope;
        return Crypto.chacha20Poly1305Open(key, body(VERSION_ATTACH, envelope), attachAad(auth));
    }

    // --- transaction_opaque_data (AAD additionally binds the parent attach) -

    /**
     * Seal {@code transaction_opaque_data} bound to both the principal and its parent attach envelope.
     *
     * @param plaintext the worker's plain transaction token
     * @param attachEnvelope the sealed attach envelope this transaction belongs to (bound into the AAD)
     * @param auth the calling principal
     * @return the sealed envelope, or {@code plaintext} unchanged when disabled / {@code null}
     */
    public byte[] sealTransaction(byte[] plaintext, byte[] attachEnvelope, AuthContext auth) {
        if (key == null || plaintext == null) return plaintext;
        return envelope(VERSION_TRANSACTION,
                Crypto.chacha20Poly1305Seal(key, plaintext, transactionAad(auth, attachEnvelope)));
    }

    /**
     * Unseal a {@code transaction_opaque_data} envelope produced by {@link #sealTransaction}.
     *
     * @param envelope the sealed transaction envelope received from the client
     * @param attachEnvelope the sealed attach envelope it must belong to; mismatch fails decryption
     * @param auth the calling principal; must match the one that sealed it
     * @return the recovered plain transaction token, or {@code envelope} unchanged when disabled / empty
     */
    public byte[] unsealTransaction(byte[] envelope, byte[] attachEnvelope, AuthContext auth) {
        if (key == null || envelope == null || envelope.length == 0) return envelope;
        return Crypto.chacha20Poly1305Open(key, body(VERSION_TRANSACTION, envelope),
                transactionAad(auth, attachEnvelope));
    }

    // --- internals ---------------------------------------------------------

    private static byte[] envelope(byte version, byte[] sealed) {
        byte[] out = new byte[1 + sealed.length];
        out[0] = version;
        System.arraycopy(sealed, 0, out, 1, sealed.length);
        return out;
    }

    private static byte[] body(byte expectedVersion, byte[] envelope) {
        if (envelope.length < 1 || envelope[0] != expectedVersion) {
            throw new IllegalArgumentException("opaque data not recognized");
        }
        return Arrays.copyOfRange(envelope, 1, envelope.length);
    }

    private static byte[] attachAad(AuthContext auth) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(ATTACH_AAD_PREFIX);
        b.writeBytes(identityTail(auth));
        return b.toByteArray();
    }

    private static byte[] transactionAad(AuthContext auth, byte[] attachEnvelope) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(TRANSACTION_AAD_PREFIX);
        b.writeBytes(identityTail(auth));
        b.write(0);
        if (attachEnvelope != null) b.writeBytes(attachEnvelope);
        return b.toByteArray();
    }

    /** {@code \x00"anonymous"} unauthenticated, else {@code \x01 domain \x00 principal}. */
    private static byte[] identityTail(AuthContext auth) {
        if (auth == null || !auth.authenticated()) {
            return new byte[] {0, 'a', 'n', 'o', 'n', 'y', 'm', 'o', 'u', 's'};
        }
        byte[] domain = (auth.domain() == null ? "" : auth.domain()).getBytes(StandardCharsets.UTF_8);
        byte[] principal = (auth.principal() == null ? "" : auth.principal()).getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(1);
        b.writeBytes(domain);
        b.write(0);
        b.writeBytes(principal);
        return b.toByteArray();
    }
}
