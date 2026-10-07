// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.identity.XChaCha20Poly1305;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;

import javax.crypto.AEADBadTagException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Attach tickets: a user's ATTACH, sealed so a runner can replay it later as that user.
 *
 * <p>A ticket is the <em>what</em> half of an unattended session; a sealed grant
 * ({@code vgi_rpc.Identity.v1} {@code issue_grant}) is the <em>who</em>. While the user is
 * attached and logged in, the client calls {@code seal_attach} ({@link AttachTicketsService}); the
 * worker seals the options it attached with -- secret ones included -- into a ticket only this
 * worker's key can open. Later a runner holding the user's grant attaches with the single option
 * {@value #OPTION}, and {@code catalog_attach} restores the sealed attach before any catalog code
 * runs. The runner never sees an option. A ticket carries no authority: it opens only under the
 * <em>caller's</em> principal.
 *
 * <p>Token ({@code docs/protocol/vgi-attach-tickets.md} in vgi-python is normative; the cross-SDK
 * vectors are {@code attach_ticket_vectors.json}):
 *
 * <pre>
 * ticket   = "vgia1." base64url_nopad(envelope)
 * envelope = 0x01 || nonce(24) || XChaCha20-Poly1305(key, nonce, payload, aad)
 * key      = VGI_SIGNING_KEY, normalized: 32 bytes as-is, otherwise SHA-256 of it
 * aad      = "vgi.attach_ticket.v1" 0x00 || UTF-8(principal)
 * </pre>
 *
 * <p>The AAD binds the principal only, not the {@code (domain, principal)} pair the attach
 * envelope binds: a ticket is sealed while the user is logged in (domain {@code jwt}, say) and
 * opened when a runner presents their grant (domain {@code grant}).
 */
public final class AttachTickets {

    /** Token prefix; the format version is in the prefix. */
    public static final String PREFIX = "vgia1.";
    /** The reserved ATTACH option a runner presents a ticket in, compared case-insensitively. */
    public static final String OPTION = "vgi_attach_ticket";
    /** Wire name of the protocol hosting {@code seal_attach}. */
    public static final String PROTOCOL_NAME = "vgi.attach_tickets.v1";
    /** Its declared protocol version. */
    public static final String PROTOCOL_VERSION = "1.0.0";
    /** The envelope version byte, fixed by this format. */
    public static final byte ENVELOPE_VERSION = 0x01;
    /** Largest options record (serialized Arrow IPC bytes) a ticket may carry. */
    public static final int MAX_OPTIONS_BYTES = 16 * 1024;
    /** Longest ticket text considered at all. */
    public static final int MAX_TICKET_CHARS = 32 * 1024;
    /** Allowance for clocks disagreeing between the sealing and the redeeming worker. */
    public static final long CLOCK_SKEW_SECONDS = 60;

    private static final byte[] AAD_DOMAIN = "vgi.attach_ticket.v1\0".getBytes(StandardCharsets.UTF_8);
    private static final Pattern TICKET_ID = Pattern.compile("[0-9a-f]{32}");
    private static final Pattern B64URL = Pattern.compile("[A-Za-z0-9_-]+");
    private static final int MAX_TEXT = 0xFFFF;
    private static final SecureRandom RNG = new SecureRandom();

    private AttachTickets() {}

    /**
     * What a ticket carries.
     *
     * @param issuedAt seconds since the Unix epoch
     * @param expiresAt seconds since the Unix epoch; {@code 0} means no expiry
     * @param ticketId 32 lowercase hex characters; a correlation handle, not a secret
     * @param catalogName the catalog the user attached
     * @param dataVersionSpec {@code ""} when the user gave none
     * @param implementationVersion {@code ""} when the user gave none
     * @param optionsIpc the options record's Arrow IPC bytes, exactly as
     *        {@code CatalogAttachRequest.options}; empty for none
     */
    public record Claims(long issuedAt, long expiresAt, String ticketId, String catalogName,
                         String dataVersionSpec, String implementationVersion, byte[] optionsIpc) {}

    /**
     * Normalize a signing key exactly as the attach envelope does: 32 bytes are used as-is, any
     * other length is replaced by its SHA-256.
     *
     * @param raw the configured key bytes (for {@code VGI_SIGNING_KEY}, the UTF-8 of its value)
     * @return the 32-byte key
     */
    public static byte[] normalizeKey(byte[] raw) {
        if (raw.length == XChaCha20Poly1305.KEY_LEN) return raw.clone();
        try {
            return MessageDigest.getInstance("SHA-256").digest(raw);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The ticket AAD: {@code "vgi.attach_ticket.v1" 0x00 || UTF-8(principal)}.
     *
     * @param principal the principal the ticket is for
     * @return the AAD bytes
     */
    public static byte[] aad(String principal) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(AAD_DOMAIN);
        out.writeBytes(principal.getBytes(StandardCharsets.UTF_8));
        return out.toByteArray();
    }

    /**
     * Seal a ticket for {@code principal} with a fresh id and nonce.
     *
     * @param signingKey the worker's signing key (any length; normalized)
     * @param principal the caller the ticket is for; non-empty
     * @param catalogName the catalog attached; non-empty
     * @param optionsIpc the options record's Arrow IPC bytes, or empty
     * @param dataVersionSpec {@code ""} for none
     * @param implementationVersion {@code ""} for none
     * @param issuedAt seconds since the Unix epoch
     * @param expiresAt seconds since the Unix epoch, or {@code 0} for no expiry
     * @return the ticket text
     * @throws IllegalArgumentException for an empty principal or catalog, an empty lifetime, or a
     *     field too long to encode
     */
    public static String mint(byte[] signingKey, String principal, String catalogName, byte[] optionsIpc,
                              String dataVersionSpec, String implementationVersion,
                              long issuedAt, long expiresAt) {
        byte[] id = new byte[16];
        RNG.nextBytes(id);
        byte[] nonce = new byte[XChaCha20Poly1305.NONCE_LEN];
        RNG.nextBytes(nonce);
        return mint(signingKey, principal, catalogName, optionsIpc, dataVersionSpec, implementationVersion,
                issuedAt, expiresAt, HexFormat.of().formatHex(id), nonce);
    }

    /**
     * Seal a ticket with a fixed id and nonce. <strong>For vectors only</strong>: a reused nonce
     * under one key breaks the cipher.
     *
     * @param signingKey the worker's signing key (any length; normalized)
     * @param principal the caller the ticket is for; non-empty
     * @param catalogName the catalog attached; non-empty
     * @param optionsIpc the options record's Arrow IPC bytes, or empty
     * @param dataVersionSpec {@code ""} for none
     * @param implementationVersion {@code ""} for none
     * @param issuedAt seconds since the Unix epoch
     * @param expiresAt seconds since the Unix epoch, or {@code 0} for no expiry
     * @param ticketId 32 lowercase hex characters
     * @param nonce 24 bytes
     * @return the ticket text
     */
    public static String mint(byte[] signingKey, String principal, String catalogName, byte[] optionsIpc,
                              String dataVersionSpec, String implementationVersion,
                              long issuedAt, long expiresAt, String ticketId, byte[] nonce) {
        if (principal == null || principal.isEmpty()) throw new IllegalArgumentException("a ticket needs a principal");
        if (catalogName == null || catalogName.isEmpty()) {
            throw new IllegalArgumentException("a ticket needs a catalog name");
        }
        if (ticketId == null || !TICKET_ID.matcher(ticketId).matches()) {
            throw new IllegalArgumentException("ticket_id must be 32 lowercase hex");
        }
        if (expiresAt != 0 && expiresAt <= issuedAt) {
            throw new IllegalArgumentException("expires_at must be 0 or after issued_at");
        }
        byte[] payload = encodePayload(new Claims(issuedAt, expiresAt, ticketId, catalogName,
                nullToEmpty(dataVersionSpec), nullToEmpty(implementationVersion),
                optionsIpc == null ? new byte[0] : optionsIpc));
        byte[] sealed = XChaCha20Poly1305.seal(normalizeKey(signingKey), nonce, payload, aad(principal));
        byte[] envelope = new byte[1 + nonce.length + sealed.length];
        envelope[0] = ENVELOPE_VERSION;
        System.arraycopy(nonce, 0, envelope, 1, nonce.length);
        System.arraycopy(sealed, 0, envelope, 1 + nonce.length, sealed.length);
        String token = PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(envelope);
        if (token.length() > MAX_TICKET_CHARS) {
            throw new IllegalArgumentException("the ticket would be " + token.length()
                    + " characters; at most " + MAX_TICKET_CHARS + " are accepted");
        }
        return token;
    }

    /**
     * The serialized payload for {@code claims} (§2.1), exposed so the vectors can pin it.
     *
     * @param claims what to encode
     * @return the little-endian payload
     * @throws IllegalArgumentException when the options exceed {@link #MAX_OPTIONS_BYTES} or a
     *     string field exceeds 65535 bytes
     */
    public static byte[] encodePayload(Claims claims) {
        if (claims.optionsIpc().length > MAX_OPTIONS_BYTES) {
            throw new IllegalArgumentException("options are " + claims.optionsIpc().length
                    + " bytes; a ticket carries at most " + MAX_OPTIONS_BYTES);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        head.putLong(claims.issuedAt()).putLong(claims.expiresAt());
        out.writeBytes(head.array());
        packText(out, claims.ticketId(), "ticket_id");
        packText(out, claims.catalogName(), "catalog_name");
        packText(out, claims.dataVersionSpec(), "data_version_spec");
        packText(out, claims.implementationVersion(), "implementation_version");
        out.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(claims.optionsIpc().length).array());
        out.writeBytes(claims.optionsIpc());
        return out.toByteArray();
    }

    /**
     * Verify a ticket for the calling principal and return what it carries.
     *
     * <p>Order, normative (§3): prefix, length, canonical base64url, caller, AEAD open under the
     * caller's principal, strict payload parse, then the lifetime with a 60 s skew. The lifetime is
     * inside the ciphertext, so it is trusted only after the tag verified.
     *
     * @param signingKey the worker's signing key (any length; normalized)
     * @param token the ticket text, exactly as presented
     * @param principal the caller's authenticated principal; {@code null} or empty never opens one
     * @param nowSeconds the current Unix time in seconds
     * @return the verified claims
     * @throws AttachTicketInvalidError for every cause but the lifetime
     * @throws AttachTicketExpiredError when authentic but outside its lifetime
     */
    public static Claims open(byte[] signingKey, String token, String principal, double nowSeconds) {
        if (token == null || !token.startsWith(PREFIX)) throw new AttachTicketInvalidError("not an attach ticket");
        if (token.length() > MAX_TICKET_CHARS) throw new AttachTicketInvalidError("attach ticket is too long");
        byte[] envelope = decodeStrict(token.substring(PREFIX.length()));
        if (principal == null || principal.isEmpty()) {
            throw new AttachTicketInvalidError("an anonymous caller cannot redeem an attach ticket");
        }
        int head = 1 + XChaCha20Poly1305.NONCE_LEN;
        if (envelope.length < head + XChaCha20Poly1305.TAG_LEN || envelope[0] != ENVELOPE_VERSION) {
            throw new AttachTicketInvalidError("attach ticket failed verification");
        }
        byte[] payload;
        try {
            payload = XChaCha20Poly1305.open(normalizeKey(signingKey), Arrays.copyOfRange(envelope, 1, head),
                    Arrays.copyOfRange(envelope, head, envelope.length), aad(principal));
        } catch (AEADBadTagException | RuntimeException e) {
            throw new AttachTicketInvalidError("attach ticket failed verification");
        }
        Claims claims = decodePayload(payload);
        if (claims.issuedAt() > nowSeconds + CLOCK_SKEW_SECONDS) {
            throw new AttachTicketExpiredError("attach ticket is not yet valid");
        }
        if (claims.expiresAt() != 0 && nowSeconds >= claims.expiresAt() + CLOCK_SKEW_SECONDS) {
            throw new AttachTicketExpiredError("attach ticket has expired");
        }
        return claims;
    }

    /**
     * Replace a ticket-carrying attach request with the attach it seals (§6).
     *
     * <p>Run by the framework at the top of {@code catalog_attach}, before any catalog code and
     * before routing on the catalog name. Never logs the ticket or a restored option.
     *
     * @param request the incoming {@code catalog_attach} request
     * @param signingKey the worker's signing key; {@code null} off HTTP, where no ticket opens
     * @param auth the caller
     * @param nowSeconds the current Unix time in seconds
     * @return {@code null} when the options carry no {@value #OPTION} (the request is untouched);
     *     otherwise the request the user originally made, with this request's
     *     {@code client_capabilities}
     * @throws farm.query.vgirpc.errors.StatusError {@code invalid_request} when another option
     *     rides alongside the ticket
     * @throws AttachTicketInvalidError when the ticket does not open for this caller
     * @throws AttachTicketExpiredError when the ticket is outside its lifetime
     */
    public static CatalogAttachRequest redeem(CatalogAttachRequest request, byte[] signingKey,
                                              AuthContext auth, double nowSeconds) {
        byte[] options = request.options();
        if (options == null || options.length == 0) return null;
        Presented presented;
        try {
            presented = BatchUtil.withReadBatch(options, Allocators.root(), AttachTickets::presented);
        } catch (RuntimeException e) {
            // Unreadable options are not a ticket; the attach fails on its own terms.
            return null;
        }
        if (presented == null || presented.ticketKeys().isEmpty()) return null;
        List<String> others = new ArrayList<>();
        for (String name : presented.names()) {
            if (!name.equals(presented.ticketKeys().get(0))) others.add(name);
        }
        if (!others.isEmpty()) {
            List<farm.query.vgirpc.errors.ErrorDetail.FieldViolation> violations = new ArrayList<>();
            for (String name : others) {
                violations.add(new farm.query.vgirpc.errors.ErrorDetail.FieldViolation(
                        "options." + name, "not allowed alongside " + OPTION));
            }
            throw invalidRequest(OPTION + " must be the only attach option", violations);
        }
        if (presented.token() == null) throw new AttachTicketInvalidError(OPTION + " must be a string");
        if (signingKey == null) throw new AttachTicketInvalidError("this worker does not redeem attach tickets");
        String principal = auth == null || !auth.authenticated() ? null : auth.principal();
        Claims claims = open(signingKey, presented.token(), principal, nowSeconds);
        byte[] restored = claims.optionsIpc().length == 0 ? null : claims.optionsIpc();
        if (restored != null) {
            try {
                BatchUtil.withReadBatch(restored, Allocators.root(), root -> null);
            } catch (RuntimeException e) {
                throw new AttachTicketInvalidError("attach ticket options are not an Arrow IPC record");
            }
        }
        return new CatalogAttachRequest(
                claims.catalogName(),
                restored,
                claims.dataVersionSpec().isEmpty() ? null : claims.dataVersionSpec(),
                claims.implementationVersion().isEmpty() ? null : claims.implementationVersion(),
                request.client_capabilities());
    }

    /**
     * Whether {@code name} is the reserved ticket option, compared case-insensitively.
     *
     * @param name an attach option name
     * @return {@code true} for any spelling of {@value #OPTION}
     */
    public static boolean isReserved(String name) {
        return name != null && name.toLowerCase(Locale.ROOT).equals(OPTION);
    }

    /** An {@code invalid_request} / {@code INVALID_ARGUMENT} error with a {@code BadRequest}. */
    static farm.query.vgirpc.errors.StatusError invalidRequest(
            String message, List<farm.query.vgirpc.errors.ErrorDetail.FieldViolation> violations) {
        return new farm.query.vgirpc.errors.StatusError(message,
                farm.query.vgirpc.errors.Code.INVALID_ARGUMENT, "invalid_request",
                List.of(new farm.query.vgirpc.errors.ErrorDetail.BadRequest(violations)));
    }

    // ------------------------------------------------------------------

    private record Presented(List<String> names, List<String> ticketKeys, String token) {}

    private static Presented presented(org.apache.arrow.vector.VectorSchemaRoot root) {
        // A zero-row record carries no options, like an absent one.
        if (root == null || root.getRowCount() == 0) return null;
        List<String> names = new ArrayList<>();
        List<String> ticketKeys = new ArrayList<>();
        for (Field f : root.getSchema().getFields()) {
            names.add(f.getName());
            if (isReserved(f.getName())) ticketKeys.add(f.getName());
        }
        String token = null;
        if (!ticketKeys.isEmpty()) {
            FieldVector v = root.getVector(ticketKeys.get(0));
            if (v instanceof VarCharVector s && !s.isNull(0)) {
                token = new String(s.get(0), StandardCharsets.UTF_8);
            }
        }
        return new Presented(names, ticketKeys, token);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static void packText(ByteArrayOutputStream out, String value, String field) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        if (raw.length > MAX_TEXT) throw new IllegalArgumentException(field + " is longer than 65535 bytes");
        out.write(raw.length & 0xFF);
        out.write((raw.length >>> 8) & 0xFF);
        out.writeBytes(raw);
    }

    private static byte[] decodeStrict(String text) {
        if (!B64URL.matcher(text).matches() || text.length() % 4 == 1) {
            throw new AttachTicketInvalidError("attach ticket is not unpadded base64url");
        }
        byte[] raw;
        try {
            raw = Base64.getUrlDecoder().decode(text);
        } catch (IllegalArgumentException e) {
            throw new AttachTicketInvalidError("attach ticket is not unpadded base64url");
        }
        if (!Base64.getUrlEncoder().withoutPadding().encodeToString(raw).equals(text)) {
            throw new AttachTicketInvalidError("attach ticket is not canonical base64url");
        }
        return raw;
    }

    /** Parse strictly: exact lengths, valid UTF-8, the field rules, no trailing bytes. */
    static Claims decodePayload(byte[] payload) {
        ByteBuffer in = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        take(in, 16);
        long issuedAt = in.getLong(0);
        long expiresAt = in.getLong(8);
        String ticketId = text(in);
        String catalogName = text(in);
        String dataVersionSpec = text(in);
        String implementationVersion = text(in);
        long optionsLen = Integer.toUnsignedLong(take(in, 4).getInt(in.position() - 4));
        if (optionsLen > MAX_OPTIONS_BYTES) throw new AttachTicketInvalidError("attach ticket options exceed 16 KiB");
        int start = in.position();
        take(in, (int) optionsLen);
        byte[] options = Arrays.copyOfRange(payload, start, start + (int) optionsLen);
        if (in.hasRemaining()) throw new AttachTicketInvalidError("attach ticket payload has trailing bytes");
        if (!TICKET_ID.matcher(ticketId).matches()) {
            throw new AttachTicketInvalidError("attach ticket id is not 32 lowercase hex");
        }
        if (catalogName.isEmpty()) throw new AttachTicketInvalidError("attach ticket names no catalog");
        if (expiresAt != 0 && expiresAt <= issuedAt) throw new AttachTicketInvalidError("attach ticket lifetime is empty");
        return new Claims(issuedAt, expiresAt, ticketId, catalogName, dataVersionSpec, implementationVersion, options);
    }

    /** Check {@code n} more bytes are present and advance past them; returns the buffer. */
    private static ByteBuffer take(ByteBuffer in, int n) {
        if (n < 0 || in.remaining() < n) throw new AttachTicketInvalidError("attach ticket payload is truncated");
        in.position(in.position() + n);
        return in;
    }

    private static String text(ByteBuffer in) {
        int len = Short.toUnsignedInt(take(in, 2).getShort(in.position() - 2));
        int start = take(in, len).position() - len;
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(in.array(), start, len)).toString();
        } catch (CharacterCodingException e) {
            throw new AttachTicketInvalidError("attach ticket payload is not UTF-8");
        }
    }
}
