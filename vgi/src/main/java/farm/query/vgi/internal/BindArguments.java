// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.protocol.ArgumentsLocation;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.Nested;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.StatusError;
import farm.query.vgirpc.stateref.ArgumentsRefIntegrityError;
import farm.query.vgirpc.stateref.ArgumentsRefMissingError;
import farm.query.vgirpc.stateref.InvalidArgumentsRefError;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Argument references (protocol 3.4): validating and resolving
 * {@code BindRequest.arguments_location} (vgi {@code docs/large-values-3.4.md} §2, §5, §5a).
 *
 * <p>A client binding a function with a large argument over HTTP uploads the arguments once,
 * through this worker's {@code __upload_url__} grant, and sends a {@code BindRequest} whose
 * {@code arguments} is null and whose {@code arguments_location} carries the grant's sealed
 * {@code ref} plus the upload's size and SHA-256. Every request that embeds that bind call
 * (cardinality, statistics, plan, projection, dynamic-to-string and init) carries the small
 * location, never the value.</p>
 *
 * <p>The framework resolves the location at the entry of each worker method that receives a
 * bind call, through {@link CallContext#resolveArgumentRef} (vgi-rpc opens the sealed ref,
 * checks size and SHA-256, and fetches through the per-process object cache, single-flight).
 * The bind call itself is never modified: whatever is carried onward (stream state, worker
 * storage, the projection envelope, split fingerprints) carries the location, never the
 * value. A {@link Nested} bind call re-encodes as exactly the bytes received.</p>
 *
 * <p>Mirrors vgi-python {@code vgi/arguments_ref.py} (33099b4).</p>
 */
public final class BindArguments {

    private BindArguments() {}

    /** The capability header advertising argument-reference support and its threshold. */
    public static final String HEADER = "VGI-Arguments-Ref-Bytes";
    /** Operator override of the advertised value. */
    public static final String BYTES_ENV = "VGI_ARGUMENTS_REF_BYTES";
    /** The default advertised value: 4 MiB. */
    public static final long DEFAULT_BYTES = 4L * 1024 * 1024;
    /** Test switch: stop advertising argument references (the extension's fallback test). */
    public static final String DISABLE_ENV = "VGI_TEST_DISABLE_ARGUMENTS_REF";

    /** Kind of the refusal for an argument object storage reports gone. */
    public static final String MISSING_KIND = "arguments_ref_missing";
    /** The exact message for it (large-values spec §5a, "Prepared statements"). */
    public static final String MISSING_MESSAGE =
            "this prepared statement's large argument has expired; prepare it again";

    /** Kind of the refusal for a bind call that does not set exactly one of the two fields. */
    public static final String INVALID_BIND_REQUEST_KIND = "invalid_bind_request";
    /** Its message, identical to vgi-python's. */
    public static final String INVALID_BIND_REQUEST_MESSAGE =
            "BindRequest must set exactly one of 'arguments' and 'arguments_location'";
    /** Kind of the refusal for a malformed or unresolvable reference. */
    public static final String INVALID_ARGUMENTS_REF_KIND = "invalid_arguments_ref";

    private static final int MAX_HEADER_DIGITS = 19;
    private static final byte ARGS_INLINE = 0x00;
    private static final byte ARGS_LOCATION = 0x01;

    /**
     * The {@code VGI-Arguments-Ref-Bytes} value to advertise.
     *
     * @param env the environment
     * @return {@code VGI_ARGUMENTS_REF_BYTES} when set, else 4 MiB
     * @throws IllegalArgumentException when the variable is not a positive decimal integer of at
     *     most 19 digits, so a typo stops startup instead of silently advertising nothing
     */
    public static long bytesFromEnv(Map<String, String> env) {
        String raw = env.get(BYTES_ENV);
        raw = raw == null ? "" : raw.strip();
        if (raw.isEmpty()) return DEFAULT_BYTES;
        if (!raw.chars().allMatch(c -> c >= '0' && c <= '9') || raw.startsWith("0")
                || raw.length() > MAX_HEADER_DIGITS) {
            throw new IllegalArgumentException(BYTES_ENV + " must be a positive integer number of bytes "
                    + "(no sign, no leading zero, at most " + MAX_HEADER_DIGITS + " digits), got '" + raw + "'");
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(BYTES_ENV + " is out of range: '" + raw + "'");
        }
    }

    /**
     * Whether the test switch {@value #DISABLE_ENV} is on.
     *
     * @param env the environment
     * @return {@code true} for {@code 1}, {@code true} or {@code yes}
     */
    public static boolean disabled(Map<String, String> env) {
        String raw = env.get(DISABLE_ENV);
        if (raw == null) return false;
        String v = raw.strip().toLowerCase(java.util.Locale.ROOT);
        return v.equals("1") || v.equals("true") || v.equals("yes");
    }

    /**
     * Require exactly one of {@code arguments} and {@code arguments_location}, and a well-formed
     * location.
     *
     * @param call the bind call
     * @throws StatusError {@code invalid_bind_request} or {@code invalid_arguments_ref}
     *     ({@code INVALID_ARGUMENT})
     */
    public static void validate(BindRequest call) {
        if ((call.arguments() == null) == (call.arguments_location() == null)) {
            throw new StatusError(INVALID_BIND_REQUEST_MESSAGE, Code.INVALID_ARGUMENT,
                    INVALID_BIND_REQUEST_KIND, List.of());
        }
        ArgumentsLocation loc = call.arguments_location();
        if (loc == null) return;
        if (loc.ref() == null || loc.ref().length == 0) {
            throw invalid("arguments_location.ref must be set");
        }
        if (loc.size() < 1) {
            throw invalid("arguments_location.size must be a positive byte count");
        }
        String sha = loc.sha256();
        if (sha == null || sha.length() != 64
                || !sha.chars().allMatch(c -> (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            throw invalid("arguments_location.sha256 must be 64 lowercase hex digits");
        }
    }

    /**
     * The bind call's arguments IPC bytes: inline, or resolved now through {@code ctx}.
     *
     * @param call the bind call as received (never modified)
     * @param ctx the turn's call context
     * @return the arguments IPC stream bytes (shared with the object cache when resolved: do not
     *     modify)
     * @throws StatusError {@code invalid_bind_request}; {@code invalid_arguments_ref}
     *     ({@code INVALID_ARGUMENT}) for a reference that does not open or match, or no context;
     *     {@code arguments_ref_integrity} ({@code DATA_LOSS}); {@code arguments_ref_missing}
     *     ({@code NOT_FOUND}) with the prepared-statement message. vgi-rpc's
     *     {@code storage_unavailable} passes through.
     */
    public static byte[] resolveIpc(BindRequest call, CallContext ctx) {
        validate(call);
        if (call.arguments() != null) return call.arguments();
        return resolveLocation(call.arguments_location(), ctx);
    }

    /**
     * Resolve an {@link ArgumentsLocation} carried onward (stream state, worker storage).
     *
     * @param loc the location
     * @param ctx the turn's call context
     * @return the verified arguments IPC bytes
     */
    public static byte[] resolveLocation(ArgumentsLocation loc, CallContext ctx) {
        if (ctx == null) {
            throw invalid("argument references need the request's context to resolve");
        }
        try {
            return ctx.resolveArgumentRef(loc.ref(), loc.size(), loc.sha256());
        } catch (ArgumentsRefMissingError e) {
            throw new StatusError(MISSING_MESSAGE, Code.NOT_FOUND, MISSING_KIND, List.of());
        } catch (InvalidArgumentsRefError | ArgumentsRefIntegrityError e) {
            // Same kind and code; the message also names the kind, because a client that shows
            // only "<type>: <message>" (the DuckDB extension) would otherwise never show it.
            throw new StatusError(e.errorKind() + ": " + e.getMessage(), e.errorCode(), e.errorKind(), List.of());
        }
    }

    /**
     * The bind call's parsed {@link Arguments}: inline, or resolved now through {@code ctx}.
     *
     * @param call the bind call as received
     * @param ctx the turn's call context
     * @return the arguments
     */
    public static Arguments resolve(BindRequest call, CallContext ctx) {
        byte[] ipc = resolveIpc(call, ctx);
        return parse(ipc, call.arguments_location() != null);
    }

    /**
     * Parse arguments IPC, refusing an uploaded stream that does not decode as
     * {@code invalid_arguments_ref} (the bytes are exactly what the client uploaded and claimed,
     * so a bad stream is the client's).
     *
     * @param ipc the arguments IPC bytes
     * @param uploaded whether they came from an argument reference
     * @return the arguments
     */
    public static Arguments parse(byte[] ipc, boolean uploaded) {
        if (!uploaded) return ArgumentsParser.parse(ipc);
        try {
            return ArgumentsParser.parse(ipc);
        } catch (RuntimeException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw invalid("the uploaded arguments are not a valid arguments IPC stream: " + cause.getMessage());
        }
    }

    /**
     * Decode and validate a {@link Nested} bind call.
     *
     * @param call the nested bind call
     * @return the decoded record (shared, read-only)
     * @throws StatusError {@code invalid_bind_request} when it does not set exactly one field
     */
    public static BindRequest decode(Nested<BindRequest> call) {
        BindRequest decoded = call.get();
        validate(decoded);
        return decoded;
    }

    /**
     * The arguments' part of the split-token bind fingerprint (large-values spec §5a,
     * "Split-token fingerprints"): an argument reference contributes
     * {@code 0x01 || u64le(size) || sha256[32]}, never the sealed {@code ref} (it holds a
     * random nonce); inline arguments contribute {@code 0x00 || u64le(len) || IPC bytes}. Plan
     * and init carry the same bind-call bytes, so both sides take the same branch.
     *
     * @param call the bind call as received
     * @return the fingerprint input
     */
    public static byte[] fingerprintInput(BindRequest call) {
        ArgumentsLocation loc = call.arguments_location();
        if (loc != null) {
            byte[] digest = HexFormat.of().parseHex(loc.sha256());
            return ByteBuffer.allocate(1 + 8 + digest.length).order(ByteOrder.LITTLE_ENDIAN)
                    .put(ARGS_LOCATION).putLong(loc.size()).put(digest).array();
        }
        byte[] raw = call.arguments() == null ? new byte[0] : call.arguments();
        return ByteBuffer.allocate(1 + 8 + raw.length).order(ByteOrder.LITTLE_ENDIAN)
                .put(ARGS_INLINE).putLong(raw.length).put(raw).array();
    }

    private static StatusError invalid(String message) {
        return new StatusError(INVALID_ARGUMENTS_REF_KIND + ": " + message, Code.INVALID_ARGUMENT,
                INVALID_ARGUMENTS_REF_KIND, List.of());
    }
}
