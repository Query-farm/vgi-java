// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.auth.Crypto;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Split-token envelope: the framework's wrapper around a worker's split payload.
 *
 * <p>A split token <em>names</em> a unit of scan work so a distributed engine can
 * re-request exactly the work it was handed. The worker supplies only the
 * payload; everything around it is stamped here, so an author cannot forget the
 * consistency anchor or mis-bind the fingerprint, and never writes crypto.</p>
 *
 * <p>Layout (little-endian, fixed prefix) — byte-identical across every SDK:</p>
 *
 * <pre>
 * offset  size  field
 * 0       1     format_version      currently 1
 * 1       1     flags               bit0 = payload_sealed; bits 1-7 reserved, MUST be 0
 * 2       2     anchor_len          u16 LE
 * 4       16    bind_fingerprint    truncated SHA-256 of the bind identity
 * 20      var   consistency_anchor  anchor_len bytes
 * 20+n    var   payload             the worker's own bytes
 * </pre>
 *
 * <p><b>The header is plaintext on every transport; only the payload is
 * sealed.</b> That is not a preference: a worker has no signing key on stdio and
 * AF_UNIX, which is DuckDB's primary path, so a header readable only through AEAD
 * would be unreadable exactly where DuckDB runs. It also matters for streaming —
 * a checkpointed position must survive key rotation.</p>
 *
 * <p><b>Sealed-payload note.</b> The header, the fingerprint and the anchor are
 * byte-identical to every other SDK, and the shared cross-SDK vectors cover them.
 * The <em>sealed payload</em> uses this platform's ChaCha20-Poly1305 construction
 * — the same one {@link OpaqueDataSealer} already uses for attach and transaction
 * envelopes, and deliberately different from the XChaCha20-Poly1305 the Python /
 * Go / Rust / TypeScript SDKs use. That is sound because a split token is minted
 * <em>and</em> verified by the same worker: it never has to open a token another
 * SDK sealed. It does mean the sealed vector in the shared fixture set is
 * parse-incompatible here, which the conformance test states explicitly rather
 * than skipping silently.</p>
 */
public final class SplitToken {

    private SplitToken() {}

    /** Envelope format version. Checked unconditionally, before anything else. */
    public static final byte FORMAT_VERSION = 1;

    /** bit0 of {@code flags}: the payload is AEAD-sealed rather than plaintext. */
    private static final int FLAG_PAYLOAD_SEALED = 0x01;

    /** bits 1-7 are reserved and MUST be zero; a set bit is a forward-compat violation. */
    private static final int RESERVED_FLAGS_MASK = 0xFE;

    private static final int FINGERPRINT_LEN = 16;
    private static final int HEADER_LEN = 4 + FINGERPRINT_LEN;

    private static final byte[] AAD_PREFIX =
            "vgi.split_token.v1\u0000".getBytes(StandardCharsets.UTF_8);

    /** Stable error-kind strings, identical across SDKs. */
    public static final String KIND_INVALID = "SPLIT_TOKEN_INVALID";
    /** The consistency anchor this token names is gone; only this one means "re-run the query". */
    public static final String KIND_EXPIRED = "SPLIT_SNAPSHOT_EXPIRED";
    /** A transaction-scoped token redeemed after commit or rollback. */
    public static final String KIND_TRANSACTION_ENDED = "SPLIT_TRANSACTION_ENDED";

    /**
     * Why a split token was refused.
     *
     * <p>The kind matters to a connector: only {@link #KIND_EXPIRED} means "re-run
     * the query", and neither kind is retriable in place. Keeping the anchor in
     * the PLAINTEXT header rather than in the AAD is what makes the distinction
     * expressible — inside the AAD both collapse into one tag-check failure.</p>
     */
    public static final class SplitTokenException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final String kind;

        SplitTokenException(String kind, String message) {
            super(message);
            this.kind = kind;
        }

        /** @return the stable error-kind string this failure carries */
        public String kind() {
            return kind;
        }
    }

    private static SplitTokenException invalid(String message) {
        return new SplitTokenException(KIND_INVALID, message);
    }

    /**
     * Derive the 16-byte binding check for a bind call.
     *
     * <p>Minted <em>and</em> verified by the same worker, so it needs
     * self-consistency only — it does not have to agree with any client, which is
     * why the cross-SDK byte fixtures do not cover it. 16 bytes is a binding
     * check, not a MAC: forgery resistance comes from the seal where a key
     * exists, and from the uid trust boundary where one does not.</p>
     *
     * @param schemaName the catalog schema the function was bound in
     * @param functionName the function's name
     * @param arguments the serialised bind arguments
     * @param settings the serialised bind settings
     * @param projection the serialised projection, or empty
     * @return the 16-byte fingerprint
     */
    public static byte[] bindFingerprint(String schemaName, String functionName,
            byte[] arguments, byte[] settings, byte[] projection) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        md.update(AAD_PREFIX);
        feed(md, "schema_name", schemaName.getBytes(StandardCharsets.UTF_8));
        feed(md, "function_name", functionName.getBytes(StandardCharsets.UTF_8));
        feed(md, "arguments", arguments == null ? new byte[0] : arguments);
        feed(md, "settings", settings == null ? new byte[0] : settings);
        feed(md, "projection_ids", projection == null ? new byte[0] : projection);
        return Arrays.copyOf(md.digest(), FINGERPRINT_LEN);
    }

    private static void feed(MessageDigest md, String label, byte[] value) {
        md.update(label.getBytes(StandardCharsets.UTF_8));
        md.update((byte) 0);
        md.update(value);
        md.update((byte) 0);
    }

    /**
     * Encode the consistency anchor.
     *
     * <p>{@code catalog_version} is the counter that MOVES within an attach, so it
     * is what a plan is pinned to; {@code resolved_data_version} is fixed at
     * attach and would say nothing about staleness.</p>
     *
     * @param catalogVersion the catalog counter this plan is pinned to
     * @return the 8-byte little-endian anchor
     */
    public static byte[] anchor(long catalogVersion) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(catalogVersion).array();
    }

    /**
     * AAD for a sealed split payload: the plaintext header plus the caller identity.
     *
     * <p>The identity half is load-bearing, not incidental — it stops a token
     * minted for one principal being replayed by another, exactly as the attach
     * envelope does. A split token names data (files, offsets, tenant partitions),
     * so dropping it here while keeping it on attach would be a regression.</p>
     */
    private static byte[] aad(byte[] header, AuthContext auth) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(header);
        b.writeBytes(identityTail(auth));
        return b.toByteArray();
    }

    /** {@code 0x00 "anonymous"} unauthenticated, else {@code 0x01 domain 0x00 principal}. */
    private static byte[] identityTail(AuthContext auth) {
        if (auth == null || !auth.authenticated()) {
            return new byte[] {0, 'a', 'n', 'o', 'n', 'y', 'm', 'o', 'u', 's'};
        }
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(1);
        String domain = auth.domain() == null ? "" : auth.domain();
        String principal = auth.principal() == null ? "" : auth.principal();
        b.writeBytes(domain.getBytes(StandardCharsets.UTF_8));
        b.write(0);
        b.writeBytes(principal.getBytes(StandardCharsets.UTF_8));
        return b.toByteArray();
    }

    /**
     * Stamp (and, when a key exists, seal) a worker payload into a split token.
     *
     * @param payload the worker's own bytes naming this unit of work
     * @param fingerprint the 16-byte bind binding check
     * @param anchorBytes the consistency anchor
     * @param signingKey the worker's 32-byte key, or {@code null} when it holds none
     * @param auth the calling principal, bound into the seal
     * @return the stamped token
     */
    public static byte[] build(byte[] payload, byte[] fingerprint, byte[] anchorBytes,
            byte[] signingKey, AuthContext auth) {
        if (fingerprint == null || fingerprint.length != FINGERPRINT_LEN) {
            throw invalid("bind_fingerprint must be " + FINGERPRINT_LEN + " bytes, got "
                    + (fingerprint == null ? 0 : fingerprint.length));
        }
        if (anchorBytes.length > 0xFFFF) {
            throw invalid("consistency_anchor too long: " + anchorBytes.length
                    + " bytes exceeds u16");
        }

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(FORMAT_VERSION);
        body.write(signingKey != null ? FLAG_PAYLOAD_SEALED : 0);
        body.write(anchorBytes.length & 0xFF);
        body.write((anchorBytes.length >>> 8) & 0xFF);
        body.writeBytes(fingerprint);
        body.writeBytes(anchorBytes);
        byte[] header = body.toByteArray();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(header);
        if (signingKey == null) {
            out.writeBytes(payload == null ? new byte[0] : payload);
        } else {
            out.writeBytes(Crypto.chacha20Poly1305Seal(signingKey,
                    payload == null ? new byte[0] : payload, aad(header, auth)));
        }
        return out.toByteArray();
    }

    /**
     * Verify a split token and return the worker's payload.
     *
     * @param token the stamped envelope, as it arrived on the init request
     * @param signingKey the worker's key, or {@code null} when it holds none
     * @param auth the calling principal
     * @param expectedFingerprint the fingerprint this bind should carry, or
     *        {@code null} to skip the bind check
     * @param currentAnchor the anchor the catalog is at now, or {@code null} to
     *        skip the staleness check
     * @return the worker's own payload
     */
    public static byte[] open(byte[] token, byte[] signingKey, AuthContext auth,
            byte[] expectedFingerprint, byte[] currentAnchor) {
        if (token == null || token.length < HEADER_LEN) {
            throw invalid("split token too short: " + (token == null ? 0 : token.length)
                    + " bytes, need at least " + HEADER_LEN);
        }
        int version = token[0] & 0xFF;
        int flags = token[1] & 0xFF;
        int anchorLen = (token[2] & 0xFF) | ((token[3] & 0xFF) << 8);

        if (version != FORMAT_VERSION) {
            throw invalid("unsupported split-token format_version " + version
                    + "; this worker speaks " + FORMAT_VERSION);
        }
        if ((flags & RESERVED_FLAGS_MASK) != 0) {
            throw invalid(String.format("split token sets reserved flag bits (flags=0x%02x)", flags));
        }
        boolean sealed = (flags & FLAG_PAYLOAD_SEALED) != 0;

        // ---- The alg:none refusal. Load-bearing; do not relax. ----
        // `flags` is attacker-controlled plaintext, so it may say "not sealed" on
        // a token an attacker wrote by hand. A keyed worker that honoured that
        // would redeem forged work without ever opening an envelope. The WORKER'S
        // OWN KEY STATE decides, never the token.
        if (signingKey != null && !sealed) {
            throw invalid("split token is unsealed but this worker holds a signing key; refusing. "
                    + "An unsealed token cannot be authenticated, so accepting one here would let "
                    + "any caller forge a split (alg:none).");
        }
        if (signingKey == null && sealed) {
            throw invalid("split token is sealed but this worker holds no signing key; "
                    + "cannot open it");
        }

        int endOfAnchor = HEADER_LEN + anchorLen;
        if (token.length < endOfAnchor) {
            throw invalid("split token truncated: anchor_len=" + anchorLen
                    + " exceeds token length " + token.length);
        }

        byte[] fingerprint = Arrays.copyOfRange(token, 4, HEADER_LEN);
        byte[] anchorBytes = Arrays.copyOfRange(token, HEADER_LEN, endOfAnchor);
        byte[] header = Arrays.copyOfRange(token, 0, endOfAnchor);
        byte[] rest = Arrays.copyOfRange(token, endOfAnchor, token.length);

        if (expectedFingerprint != null
                && !Crypto.constantTimeEquals(fingerprint, expectedFingerprint)) {
            throw invalid("split token was minted for a different bind (fingerprint mismatch)");
        }
        // Anchor check AFTER the bind check, and as its own kind: "read version N"
        // is a different situation from "this token is not yours".
        if (currentAnchor != null && !Crypto.constantTimeEquals(anchorBytes, currentAnchor)) {
            throw new SplitTokenException(KIND_EXPIRED, "split snapshot expired; re-run the query");
        }

        if (!sealed) {
            return rest;
        }
        try {
            return Crypto.chacha20Poly1305Open(signingKey, rest, aad(header, auth));
        } catch (RuntimeException e) {
            throw invalid("split token failed authentication: " + e.getMessage());
        }
    }
}
