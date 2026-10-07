// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgi.AttachTickets;
import farm.query.vgi.AttachTicketsService;
import farm.query.vgi.CatalogInterface;
import farm.query.vgi.Worker;
import farm.query.vgi.protocol.AttachTicket;
import farm.query.vgi.protocol.SealAttachRequest;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.ErrorDetail;
import farm.query.vgirpc.errors.StatusError;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * {@code vgi.attach_tickets.v1} over a {@link Worker} ({@code docs/protocol/vgi-attach-tickets.md}
 * §5). Seals with the worker's signing key -- the key that seals {@code attach_opaque_data} -- so
 * tickets live exactly as long as attach envelopes do.
 */
public final class AttachTicketsImpl implements AttachTicketsService {

    private final Worker worker;
    private final byte[] signingKey;
    private final Long maxTtlSeconds;

    /**
     * Build the implementation.
     *
     * @param worker the worker whose catalogs are sealed
     * @param signingKey the worker's configured signing key
     * @param maxTtlSeconds the lifetime ceiling (the grant maximum), or {@code null} for none
     */
    public AttachTicketsImpl(Worker worker, byte[] signingKey, Long maxTtlSeconds) {
        this.worker = worker;
        this.signingKey = signingKey.clone();
        this.maxTtlSeconds = maxTtlSeconds;
    }

    @Override
    public AttachTicket seal_attach(SealAttachRequest request, CallContext ctx) {
        AuthContext auth = ctx == null ? AuthContext.ANONYMOUS : ctx.auth();
        String principal = auth != null && auth.authenticated() && auth.principal() != null
                && !auth.principal().isEmpty() ? auth.principal() : null;
        if (principal == null) {
            throw new StatusError("an anonymous caller cannot seal an attach ticket", Code.PERMISSION_DENIED,
                    "action_denied", List.of(new ErrorDetail.ErrorInfo(Map.of("action", "seal_attach"))));
        }

        List<ErrorDetail.FieldViolation> violations = new ArrayList<>();
        if (request.ttl_seconds() < 0) {
            violations.add(new ErrorDetail.FieldViolation("ttl_seconds", "must be 0 (as long as allowed) or positive"));
        }

        List<String> names = List.of();
        byte[] raw = request.options();
        if (raw != null && raw.length > 0) {
            Options read = readOptions(raw);
            if (read == null) {
                violations.add(new ErrorDetail.FieldViolation("options", "must be an Arrow IPC record"));
            } else if (read.rows() > 1) {
                violations.add(new ErrorDetail.FieldViolation("options", "must be a one-row record"));
            } else if (read.rows() == 1) {
                names = read.names();
            }
        }

        List<AttachOptionSpec> specs = declaredSpecs(request.catalog_name());
        if (specs == null) {
            violations.add(new ErrorDetail.FieldViolation("catalog_name",
                    "no catalog named '" + request.catalog_name() + "'"));
        } else {
            Set<String> declared = new HashSet<>();
            for (AttachOptionSpec spec : specs) declared.add(spec.name().toLowerCase(Locale.ROOT));
            Set<String> supplied = new HashSet<>();
            for (String name : names) {
                supplied.add(name.toLowerCase(Locale.ROOT));
                if (AttachTickets.isReserved(name)) {
                    violations.add(new ErrorDetail.FieldViolation("options." + name,
                            "a ticket cannot seal another ticket"));
                } else if (!declared.contains(name.toLowerCase(Locale.ROOT))) {
                    violations.add(new ErrorDetail.FieldViolation("options." + name,
                            "not an attach option this catalog declares"));
                }
            }
            for (AttachOptionSpec spec : specs) {
                if (spec.required() && !supplied.contains(spec.name().toLowerCase(Locale.ROOT))) {
                    violations.add(new ErrorDetail.FieldViolation("options." + spec.name(), "required"));
                }
            }
        }

        byte[] optionsIpc = names.isEmpty() ? new byte[0] : raw;
        if (optionsIpc.length > AttachTickets.MAX_OPTIONS_BYTES) {
            violations.add(new ErrorDetail.FieldViolation("options", optionsIpc.length
                    + " bytes; a ticket carries at most " + AttachTickets.MAX_OPTIONS_BYTES));
        }
        if (!violations.isEmpty()) throw invalidRequest(violations);

        long issuedAt = System.currentTimeMillis() / 1000;
        long ttl = request.ttl_seconds();
        // 0 asks for the ceiling; otherwise the request, capped at the ceiling.
        Long lifetime = ttl == 0 ? maxTtlSeconds : (maxTtlSeconds == null ? ttl : Math.min(ttl, maxTtlSeconds));
        long expiresAt = lifetime == null ? 0 : issuedAt + lifetime;
        String token;
        try {
            token = AttachTickets.mint(signingKey, principal, request.catalog_name(), optionsIpc,
                    request.data_version_spec(), request.implementation_version(), issuedAt, expiresAt);
        } catch (IllegalArgumentException e) {
            throw invalidRequest(List.of(new ErrorDetail.FieldViolation("request", e.getMessage())));
        }
        return new AttachTicket(token, expiresAt == 0 ? Double.POSITIVE_INFINITY : (double) expiresAt);
    }

    private static StatusError invalidRequest(List<ErrorDetail.FieldViolation> violations) {
        return new StatusError("seal_attach request is invalid", Code.INVALID_ARGUMENT, "invalid_request",
                List.of(new ErrorDetail.BadRequest(violations)));
    }

    /** The attach options {@code catalogName} declares, or {@code null} when no such catalog. */
    private List<AttachOptionSpec> declaredSpecs(String catalogName) {
        if (catalogName == null) return null;
        if (catalogName.equals(worker.catalogName())) return worker.attachOptionSpecs();
        Worker.ExtraCatalog extra = worker.extraCatalogs().get(catalogName);
        if (extra != null) return extra.attachOptions();
        CatalogInterface hosted = worker.catalogInterfaces().get(catalogName);
        if (hosted != null) return List.of();
        return null;
    }

    private record Options(int rows, List<String> names) {}

    private static Options readOptions(byte[] raw) {
        try {
            return BatchUtil.withReadBatch(raw, Allocators.root(), root -> {
                if (root == null) return new Options(0, List.of());
                List<String> names = new ArrayList<>();
                for (Field f : root.getSchema().getFields()) names.add(f.getName());
                return new Options(root.getRowCount(), names);
            });
        } catch (RuntimeException e) {
            return null;
        }
    }
}
