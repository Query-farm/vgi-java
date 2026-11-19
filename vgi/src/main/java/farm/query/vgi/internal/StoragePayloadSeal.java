// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.function.Supplier;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Authenticated encryption of framework payloads written to
 * {@link farm.query.vgi.storage.FunctionStorage}: sealed framework payload v1,
 * the same format in every VGI SDK.
 *
 * <p>The framework persists some of its own context to shared storage so any
 * worker process can serve any RPC of an execution: the buffered-table init
 * (bind arguments, settings, schemas and the resolved secret values) and
 * aggregate constant arguments. Storage may be a SQLite file or a Cloudflare
 * Durable Object reached over the network, so none of it may sit there in
 * plaintext, and nothing read back may be trusted unauthenticated.
 *
 * <ul>
 *   <li>{@code K = HKDF-SHA256(ikm, salt = "", info = "vgi.storage.payload.v1", L = 32)};
 *       {@code ikm} is the worker's signing key exactly as configured (the UTF-8
 *       bytes of {@code VGI_SIGNING_KEY}, or the bytes given to
 *       {@code Worker.signingKey}), else the per-user host-local key file
 *       ({@link HostLocalKey}) every VGI worker process of the user on the host
 *       shares.</li>
 *   <li>AES-256-GCM, random 12-byte nonce, 16-byte tag.</li>
 *   <li>{@code AAD = "vgi.storage.payload.v1" || u32be(len) || shard_key ||
 *       u32be(len) || scope_id || u32be(len) || namespace || u32be(len) || key}:
 *       a payload copied to another execution, slot or attach shard fails to open.</li>
 *   <li>Wire: {@code 0x01 || nonce(12) || ciphertext || tag(16)}.</li>
 * </ul>
 *
 * <p>A payload that fails to open (unknown version, wrong length, tampered,
 * wrong key, moved, or an unsealed value written by an older release) raises
 * {@link StoragePayloadException}; there is no plaintext fallback. Executions
 * are short-lived and their ids never reused, so no migration is needed.
 *
 * <p>Data user code writes to storage (buffering appends, aggregate group
 * states, queue items) is the function's own data and is not sealed.
 */
public final class StoragePayloadSeal {

    /** HKDF info label and AAD prefix. */
    public static final byte[] INFO = "vgi.storage.payload.v1".getBytes(StandardCharsets.US_ASCII);
    /** The envelope version byte. */
    public static final byte VERSION = 0x01;
    private static final int KEY_LEN = 32;
    private static final int NONCE_LEN = 12;
    private static final int TAG_LEN = 16;
    private static final SecureRandom RNG = new SecureRandom();

    /** A framework payload read from storage could not be authenticated. */
    public static final class StoragePayloadException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        StoragePayloadException(String message) {
            super(message);
        }

        StoragePayloadException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final Supplier<byte[]> ikmSource;
    private volatile byte[] key;

    private StoragePayloadSeal(Supplier<byte[]> ikmSource) {
        this.ikmSource = ikmSource;
    }

    /**
     * A sealer over an explicit signing key, or the host-local key when {@code ikm} is {@code null}.
     * The host-local key is only resolved on first use.
     *
     * @param ikm the signing key bytes exactly as configured, or {@code null}
     * @return the sealer
     */
    public static StoragePayloadSeal forKey(byte[] ikm) {
        if (ikm != null) {
            byte[] copy = ikm.clone();
            return new StoragePayloadSeal(() -> copy);
        }
        return new StoragePayloadSeal(HostLocalKey::get);
    }

    private byte[] key() {
        byte[] k = key;
        if (k == null) {
            byte[] ikm;
            try {
                ikm = ikmSource.get();
            } catch (RuntimeException e) {
                throw new StoragePayloadException(
                        "no key to seal framework storage payloads: " + e.getMessage(), e);
            }
            k = deriveKey(ikm);
            key = k;
        }
        return k;
    }

    /**
     * {@code HKDF-SHA256(ikm, salt = "", info = "vgi.storage.payload.v1", 32)}.
     *
     * @param ikm the input keying material
     * @return the 32-byte AES key
     */
    public static byte[] deriveKey(byte[] ikm) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            // RFC 5869: an empty salt is HashLen zero bytes.
            mac.init(new SecretKeySpec(new byte[32], "HmacSHA256"));
            byte[] prk = mac.doFinal(ikm);
            mac.init(new SecretKeySpec(prk, "HmacSHA256"));
            mac.update(INFO);
            mac.update((byte) 1);
            return Arrays.copyOf(mac.doFinal(), KEY_LEN);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    /**
     * The associated data binding a payload to its storage slot.
     *
     * @param shardKey the attach shard ({@code ""} when unsharded)
     * @param scopeId the execution (or scope) id
     * @param ns the storage namespace
     * @param key the key within the namespace
     * @return the AAD bytes
     */
    public static byte[] aad(String shardKey, byte[] scopeId, byte[] ns, byte[] key) {
        byte[] shard = (shardKey == null ? "" : shardKey).getBytes(StandardCharsets.UTF_8);
        byte[] scope = scopeId == null ? new byte[0] : scopeId;
        ByteBuffer b = ByteBuffer.allocate(INFO.length + 16 + shard.length + scope.length + ns.length + key.length);
        b.put(INFO);
        b.putInt(shard.length).put(shard);
        b.putInt(scope.length).put(scope);
        b.putInt(ns.length).put(ns);
        b.putInt(key.length).put(key);
        return b.array();
    }

    /**
     * Seal {@code payload} to its slot.
     *
     * @param payload the plaintext
     * @param shardKey the attach shard
     * @param scopeId the execution id
     * @param ns the namespace
     * @param key the key
     * @return the envelope to store
     */
    public byte[] seal(byte[] payload, String shardKey, byte[] scopeId, byte[] ns, byte[] key) {
        byte[] nonce = new byte[NONCE_LEN];
        RNG.nextBytes(nonce);
        return sealWith(key(), nonce, payload, aad(shardKey, scopeId, ns, key));
    }

    /**
     * Open an envelope from {@link #seal} for the same slot.
     *
     * @param blob the stored envelope
     * @param shardKey the attach shard
     * @param scopeId the execution id
     * @param ns the namespace
     * @param key the key
     * @return the plaintext
     * @throws StoragePayloadException on any failure to authenticate
     */
    public byte[] open(byte[] blob, String shardKey, byte[] scopeId, byte[] ns, byte[] key) {
        String what = "framework storage payload " + new String(ns, StandardCharsets.UTF_8)
                + " for execution " + (scopeId == null ? "" : HexFormat.of().formatHex(scopeId));
        return openWith(key(), blob, aad(shardKey, scopeId, ns, key), what);
    }

    /**
     * Seal under an explicit derived key and nonce (test vectors).
     *
     * @param aesKey the 32-byte derived key
     * @param nonce the 12-byte nonce
     * @param payload the plaintext
     * @param aad the associated data
     * @return the envelope
     */
    static byte[] sealWith(byte[] aesKey, byte[] nonce, byte[] payload, byte[] aad) {
        if (nonce.length != NONCE_LEN) throw new IllegalArgumentException("nonce must be 12 bytes");
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(aesKey, "AES"), new GCMParameterSpec(TAG_LEN * 8, nonce));
            c.updateAAD(aad);
            byte[] ct = c.doFinal(payload);
            byte[] out = new byte[1 + NONCE_LEN + ct.length];
            out[0] = VERSION;
            System.arraycopy(nonce, 0, out, 1, NONCE_LEN);
            System.arraycopy(ct, 0, out, 1 + NONCE_LEN, ct.length);
            return out;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM unavailable", e);
        }
    }

    static byte[] openWith(byte[] aesKey, byte[] blob, byte[] aad, String what) {
        if (blob == null || blob.length < 1 + NONCE_LEN + TAG_LEN) {
            throw new StoragePayloadException(what + " is too short to be a sealed payload");
        }
        if (blob[0] != VERSION) {
            throw new StoragePayloadException(what + " has unknown seal version "
                    + String.format("0x%02x", blob[0] & 0xff));
        }
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(aesKey, "AES"),
                    new GCMParameterSpec(TAG_LEN * 8, blob, 1, NONCE_LEN));
            c.updateAAD(aad);
            return c.doFinal(blob, 1 + NONCE_LEN, blob.length - 1 - NONCE_LEN);
        } catch (javax.crypto.AEADBadTagException e) {
            throw new StoragePayloadException(
                    what + " failed authentication (tampered, moved, or sealed under another key)", e);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM unavailable", e);
        }
    }
}
