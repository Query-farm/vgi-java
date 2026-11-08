// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encodes ATTACH-time option values into the returned {@code attach_id} so
 * the echo function is stateless under pool reuse / HTTP transport.
 *
 * <p>Wire format: {@code uuid(16) || 0x00 || ipc(mergedBatch)}.
 * {@code mergedBatch} is a one-row record batch with one column per declared
 * spec (declaration order). User-supplied values override the spec's default;
 * missing values fall back to the default vector materialised at spec
 * registration.
 *
 * <p><b>Secret options never appear in plaintext.</b> A spec declared {@code secret} is
 * left out of the merged batch on every transport: on stdio / AF_UNIX the attach id
 * travels unsealed, and a credential must not sit in plaintext in a value the client
 * stores and returns ({@code docs/protocol/vgi-opaque-data-sealing.md} rule 5). When the
 * caller supplies a key ({@link #encode(List, byte[], SecureRandom, byte[])}), the secret
 * options' values are instead AEAD-encrypted (ChaCha20-Poly1305, AAD-bound to this attach
 * id's uuid) into the merged batch's schema metadata under {@link #SECRET_OPTIONS_KEY}, and
 * {@link #openSecrets} recovers them with the same key. Readers that ignore schema metadata
 * see exactly the non-secret batch they always did.
 */
public final class AttachOptionsAttachId {

    private static final byte SEP = 0x00;
    private static final int UUID_BYTES = 16;

    /** Schema-metadata key of the encrypted secret-option batch (base64). */
    public static final String SECRET_OPTIONS_KEY = "vgi.secret_options";
    private static final byte[] SECRET_AAD_PREFIX =
            "vgi.attach_secret_options.v1\0".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    private AttachOptionsAttachId() {}

    /**
     * Encode the resolved option values into a fresh {@code attach_id}.
     *
     * @param declared       declared option specs (define column order and defaults);
     *                       secret ones are dropped
     * @param userOptionsIpc IPC batch of user-supplied option values, or {@code null}/empty
     * @param rng            source of the 16-byte uniqueness prefix
     * @return the encoded {@code attach_id} bytes
     */
    public static byte[] encode(List<AttachOptionSpec> declared, byte[] userOptionsIpc,
                                  SecureRandom rng) {
        return encode(declared, userOptionsIpc, rng, (java.util.function.Supplier<byte[]>) null);
    }

    /**
     * Encode the resolved option values, encrypting the secret ones with {@code secretKey}.
     *
     * @param declared       declared option specs (define column order and defaults)
     * @param userOptionsIpc IPC batch of user-supplied option values, or {@code null}/empty
     * @param rng            source of the 16-byte uniqueness prefix
     * @param secretKey      32-byte key the secret values are encrypted with, or {@code null}
     *                       to drop them
     * @return the encoded {@code attach_id} bytes
     */
    public static byte[] encode(List<AttachOptionSpec> declared, byte[] userOptionsIpc,
                                  SecureRandom rng, byte[] secretKey) {
        return encode(declared, userOptionsIpc, rng, secretKey == null ? null : () -> secretKey);
    }

    /**
     * Encode the resolved option values, encrypting the secret ones with the key
     * {@code secretKey} supplies. The supplier is called only when a secret option has a
     * value, so a worker whose secret options were not given never needs a key.
     *
     * @param declared       declared option specs (define column order and defaults)
     * @param userOptionsIpc IPC batch of user-supplied option values, or {@code null}/empty
     * @param rng            source of the 16-byte uniqueness prefix
     * @param secretKey      supplies the 32-byte key, or {@code null} to drop secret values
     * @return the encoded {@code attach_id} bytes
     * @throws IllegalStateException when a secret option has a value and no key is available
     */
    public static byte[] encode(List<AttachOptionSpec> declared, byte[] userOptionsIpc,
                                  SecureRandom rng, java.util.function.Supplier<byte[]> secretKey) {
        BufferAllocator alloc = Allocators.root();
        List<AttachOptionSpec> specs = new ArrayList<>(declared.size());
        List<AttachOptionSpec> secretSpecs = new ArrayList<>();
        for (AttachOptionSpec spec : declared) {
            if (!spec.secret()) specs.add(spec);
            else secretSpecs.add(spec);
        }

        Map<String, FieldVector> userCols = new HashMap<>();
        VectorSchemaRoot userRoot = null;
        if (userOptionsIpc != null && userOptionsIpc.length > 0) {
            userRoot = BatchUtil.readSingleBatch(userOptionsIpc, alloc);
            if (userRoot != null) {
                for (FieldVector fv : userRoot.getFieldVectors()) {
                    userCols.put(fv.getField().getName(), fv);
                }
            }
        }
        try {
            byte[] uuid = new byte[UUID_BYTES];
            rng.nextBytes(uuid);
            Map<String, String> metadata = null;
            if (secretKey != null && !secretSpecs.isEmpty()) {
                boolean anyValue = false;
                for (AttachOptionSpec spec : secretSpecs) {
                    if (pickSource(userCols.get(spec.name()), spec.defaultVector()) != null) anyValue = true;
                }
                if (anyValue) {
                    byte[] key = secretKey.get();
                    if (key == null || key.length != 32) {
                        throw new IllegalStateException("secret ATTACH options were given but this worker"
                                + " has no key to encrypt them with; set VGI_SIGNING_KEY");
                    }
                    byte[] secretIpc = mergedBatch(secretSpecs, userCols, alloc, null);
                    byte[] sealed = farm.query.vgirpc.http.auth.Crypto.chacha20Poly1305Seal(
                            key, secretIpc, secretAad(uuid));
                    metadata = Map.of(SECRET_OPTIONS_KEY, java.util.Base64.getEncoder().encodeToString(sealed));
                }
            }
            byte[] ipcBytes = mergedBatch(specs, userCols, alloc, metadata);
            byte[] attachId = new byte[UUID_BYTES + 1 + ipcBytes.length];
            System.arraycopy(uuid, 0, attachId, 0, UUID_BYTES);
            attachId[UUID_BYTES] = SEP;
            System.arraycopy(ipcBytes, 0, attachId, UUID_BYTES + 1, ipcBytes.length);
            return attachId;
        } finally {
            if (userRoot != null) userRoot.close();
        }
    }

    /** One row, one column per spec in declaration order: the user value, else the default. */
    private static byte[] mergedBatch(List<AttachOptionSpec> specs, Map<String, FieldVector> userCols,
                                      BufferAllocator alloc, Map<String, String> metadata) {
        List<Field> mergedFields = new ArrayList<>(specs.size());
        for (AttachOptionSpec spec : specs) {
            mergedFields.add(new Field(spec.name(),
                    new FieldType(true, spec.type(), null),
                    spec.children()));
        }
        Schema mergedSchema = new Schema(mergedFields, metadata);
        try (VectorSchemaRoot mergedRoot = VectorSchemaRoot.create(mergedSchema, alloc)) {
            mergedRoot.allocateNew();
            for (int i = 0; i < specs.size(); i++) {
                AttachOptionSpec spec = specs.get(i);
                FieldVector mergedVec = mergedRoot.getVector(i);
                FieldVector source = pickSource(userCols.get(spec.name()), spec.defaultVector());
                if (source == null) {
                    mergedVec.setNull(0);
                    continue;
                }
                // Uniform copy: user value or default both flow through TransferPair.
                TransferPair tp = source.makeTransferPair(mergedVec);
                tp.copyValueSafe(0, 0);
            }
            mergedRoot.setRowCount(1);
            return BatchUtil.writeSingleBatch(mergedRoot);
        }
    }

    private static byte[] secretAad(byte[] uuid) {
        byte[] aad = new byte[SECRET_AAD_PREFIX.length + UUID_BYTES];
        System.arraycopy(SECRET_AAD_PREFIX, 0, aad, 0, SECRET_AAD_PREFIX.length);
        System.arraycopy(uuid, 0, aad, SECRET_AAD_PREFIX.length, UUID_BYTES);
        return aad;
    }

    /**
     * Whether {@code attachId} has the options layout ({@code uuid || 0x00 || ipc}).
     *
     * @param attachId a plain attach id, or {@code null}
     * @return true when it carries an options batch
     */
    public static boolean carriesOptions(byte[] attachId) {
        return attachId != null && attachId.length > UUID_BYTES + 1 && attachId[UUID_BYTES] == SEP
                && attachId.length >= UUID_BYTES + 1 + 8
                && (attachId[UUID_BYTES + 1] & 0xFF) == 0xFF && (attachId[UUID_BYTES + 2] & 0xFF) == 0xFF
                && (attachId[UUID_BYTES + 3] & 0xFF) == 0xFF && (attachId[UUID_BYTES + 4] & 0xFF) == 0xFF;
    }

    /**
     * Whether {@code attachId} carries encrypted secret-option values.
     *
     * @param attachId a plain attach id, or {@code null}
     * @return true when its options batch carries {@link #SECRET_OPTIONS_KEY}
     */
    public static boolean hasSecrets(byte[] attachId) {
        return sealedSecrets(attachId) != null;
    }

    private static byte[] sealedSecrets(byte[] attachId) {
        if (!carriesOptions(attachId)) return null;
        try (org.apache.arrow.vector.ipc.ArrowStreamReader reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                new java.io.ByteArrayInputStream(attachId, UUID_BYTES + 1, attachId.length - UUID_BYTES - 1),
                Allocators.root())) {
            Map<String, String> md = reader.getVectorSchemaRoot().getSchema().getCustomMetadata();
            String b64 = md == null ? null : md.get(SECRET_OPTIONS_KEY);
            return b64 == null ? null : java.util.Base64.getDecoder().decode(b64);
        } catch (java.io.IOException | RuntimeException e) {
            return null;
        }
    }

    /**
     * Decrypt the secret-option batch {@link #encode(List, byte[], SecureRandom, byte[])} put
     * in {@code attachId}; caller closes the root.
     *
     * @param attachId the encoded attach id
     * @param key      the 32-byte key it was encrypted with
     * @param alloc    allocator for the returned root
     * @return the one-row secret batch, or {@code null} when the attach id carries none
     * @throws IllegalArgumentException when the batch does not open under {@code key}
     */
    public static VectorSchemaRoot openSecrets(byte[] attachId, byte[] key, BufferAllocator alloc) {
        byte[] sealed = sealedSecrets(attachId);
        if (sealed == null) return null;
        byte[] uuid = java.util.Arrays.copyOf(attachId, UUID_BYTES);
        byte[] ipc;
        try {
            ipc = farm.query.vgirpc.http.auth.Crypto.chacha20Poly1305Open(key, sealed, secretAad(uuid));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("secret ATTACH options do not open under this worker's key");
        }
        return BatchUtil.readSingleBatch(ipc, alloc);
    }

    private static FieldVector pickSource(FieldVector userVec, FieldVector defaultVec) {
        if (userVec != null && userVec.getValueCount() > 0 && !userVec.isNull(0)) {
            return userVec;
        }
        return defaultVec;
    }

    /**
     * Recover the merged one-row record batch IPC bytes from an attach_id.
     *
     * @param attachId the encoded attach_id
     * @return the embedded IPC batch bytes
     * @throws IllegalArgumentException if the attach_id carries no options payload
     */
    public static byte[] extractBatchIpc(byte[] attachId) {
        if (attachId == null
                || attachId.length <= UUID_BYTES + 1
                || attachId[UUID_BYTES] != SEP) {
            throw new IllegalArgumentException("attach_id does not carry options payload");
        }
        byte[] out = new byte[attachId.length - UUID_BYTES - 1];
        System.arraycopy(attachId, UUID_BYTES + 1, out, 0, out.length);
        return out;
    }

    /**
     * Read the merged record batch out of an attach_id; caller closes the root.
     *
     * @param attachId the encoded attach_id
     * @param alloc    allocator for the returned root
     * @return the decoded one-row record batch (caller owns it)
     */
    public static VectorSchemaRoot decode(byte[] attachId, BufferAllocator alloc) {
        return BatchUtil.readSingleBatch(extractBatchIpc(attachId), alloc);
    }
}
