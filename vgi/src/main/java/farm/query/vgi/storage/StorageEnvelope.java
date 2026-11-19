// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import farm.query.vgirpc.vendor.BlobRef;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.regex.Pattern;

/**
 * The v1 function-storage value envelope, byte-identical to vgi-python's
 * {@code vgi.function_storage_blobs} (vectors: the vgi extension's
 * {@code test/vectors/storage_envelope_v1.json}).
 *
 * <pre>
 * inline:  0x00 || raw
 * pointer: 0x01 || u64be(size) || sha256(raw) (32 bytes) || object_id (ASCII)
 * </pre>
 *
 * <p>The object id matches {@code [A-Za-z0-9._:-]{1,128}}; the fixed part of a
 * pointer is 41 bytes. Decoding is strict: an empty value, an unknown tag, a
 * pointer of 41 bytes or fewer, or an object id outside the charset is a
 * {@link StorageEnvelopeException}.</p>
 *
 * <p>Also here: the vendor scope names ({@link #blobExecutionId} and
 * {@link #blobShard}), which follow the same contract.</p>
 */
public final class StorageEnvelope {

    private StorageEnvelope() {}

    /** Tag of an inline value. */
    public static final int TAG_INLINE = 0x00;
    /** Tag of a blob pointer. */
    public static final int TAG_POINTER = 0x01;
    /** Bytes of a pointer before the object id: tag, u64 size, sha256. */
    public static final int POINTER_FIXED = 1 + 8 + 32;

    private static final Pattern OBJECT_ID = Pattern.compile("[A-Za-z0-9._:-]{1,128}");
    // The vendor's scope.shard charset as the wrapper uses it (the vendor also
    // accepts ':', the wrapper does not).
    private static final Pattern SHARD = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final HexFormat HEX = HexFormat.of();

    /**
     * The inline envelope of {@code value}.
     *
     * @param value the raw value
     * @return {@code 0x00 || value}
     */
    public static byte[] encodeInline(byte[] value) {
        byte[] out = new byte[value.length + 1];
        out[0] = TAG_INLINE;
        System.arraycopy(value, 0, out, 1, value.length);
        return out;
    }

    /**
     * The pointer envelope for a vendored blob.
     *
     * @param ref the blob's reference from the vendor
     * @return {@code 0x01 || u64be(size) || sha256 || object_id}
     * @throws IllegalArgumentException if the object id is outside the pointer
     *     charset or the SHA-256 is not 32 bytes of hex
     */
    public static byte[] encodePointer(BlobRef ref) {
        if (!OBJECT_ID.matcher(ref.objectId()).matches()) {
            throw new IllegalArgumentException("blob vendor returned an unusable object id '" + ref.objectId() + "'");
        }
        byte[] sha = HEX.parseHex(ref.sha256());
        if (sha.length != 32) throw new IllegalArgumentException("blob SHA-256 must be 32 bytes");
        byte[] id = ref.objectId().getBytes(StandardCharsets.US_ASCII);
        ByteBuffer buf = ByteBuffer.allocate(POINTER_FIXED + id.length);
        buf.put((byte) TAG_POINTER).putLong(ref.size()).put(sha).put(id);
        return buf.array();
    }

    /**
     * The envelope's tag.
     *
     * @param envelope a stored value
     * @return {@link #TAG_INLINE} or {@link #TAG_POINTER}
     * @throws StorageEnvelopeException if the value is empty or the tag is unknown
     */
    public static int tag(byte[] envelope) {
        if (envelope.length == 0) {
            throw new StorageEnvelopeException(
                    "empty stored value: not a value envelope (written without the envelope?)");
        }
        int tag = envelope[0] & 0xFF;
        if (tag != TAG_INLINE && tag != TAG_POINTER) {
            throw new StorageEnvelopeException(String.format(
                    "stored value has unknown envelope tag 0x%02x (written by an older client or without the "
                            + "envelope? mixing client versions within one query is not supported)", tag));
        }
        return tag;
    }

    /**
     * The raw value of an inline envelope.
     *
     * @param envelope a stored value whose tag is {@link #TAG_INLINE}
     * @return the bytes after the tag
     */
    public static byte[] inlineValue(byte[] envelope) {
        return Arrays.copyOfRange(envelope, 1, envelope.length);
    }

    /**
     * Parse a pointer envelope.
     *
     * @param envelope a stored value whose tag is {@link #TAG_POINTER}
     * @return the blob reference it names
     * @throws StorageEnvelopeException if the pointer is truncated or its object id is malformed
     */
    public static BlobRef decodePointer(byte[] envelope) {
        if (envelope.length <= POINTER_FIXED) {
            throw new StorageEnvelopeException("malformed blob pointer in stored value (" + envelope.length + " bytes)");
        }
        byte[] idBytes = Arrays.copyOfRange(envelope, POINTER_FIXED, envelope.length);
        for (byte b : idBytes) {
            if (b < 0) {
                throw new StorageEnvelopeException(
                        "malformed blob pointer in stored value (" + envelope.length + " bytes)");
            }
        }
        String id = new String(idBytes, StandardCharsets.US_ASCII);
        if (!OBJECT_ID.matcher(id).matches()) {
            throw new StorageEnvelopeException("malformed blob pointer in stored value (" + envelope.length + " bytes)");
        }
        long size = ByteBuffer.wrap(envelope, 1, 8).getLong();
        if (size < 0) {
            throw new StorageEnvelopeException("malformed blob pointer in stored value (" + envelope.length + " bytes)");
        }
        return new BlobRef(id, size, HEX.formatHex(envelope, 9, POINTER_FIXED));
    }

    /**
     * The vendor {@code scope.execution_id} for a queue ({@code q}) or a state
     * scope ({@code s}): {@code <kind>.<hex ident>}, or
     * {@code <kind>.h<hex sha256(ident)>} when that would exceed 128 characters.
     *
     * @param kind  {@code "q"} or {@code "s"}
     * @param ident the execution id (queue) or scope id (state, log)
     * @return the execution id
     */
    public static String blobExecutionId(String kind, byte[] ident) {
        String text = kind + "." + HEX.formatHex(ident);
        if (text.length() <= 128) return text;
        return kind + ".h" + HEX.formatHex(sha256(ident));
    }

    /**
     * The vendor {@code scope.shard} for a storage shard key: an empty key is
     * {@code loc-anon}; a key outside {@code [A-Za-z0-9._-]{1,128}} becomes
     * {@code h-} plus the hex SHA-256 of its UTF-8 bytes.
     *
     * @param shardKey the storage shard key
     * @return the vendor shard
     */
    public static String blobShard(String shardKey) {
        String shard = shardKey == null || shardKey.isEmpty() ? "loc-anon" : shardKey;
        if (SHARD.matcher(shard).matches()) return shard;
        return "h-" + HEX.formatHex(sha256(shard.getBytes(StandardCharsets.UTF_8)));
    }

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
