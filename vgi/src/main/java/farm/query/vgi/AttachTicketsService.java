// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.AttachTicket;
import farm.query.vgi.protocol.SealAttachRequest;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.schema.ProtocolName;
import farm.query.vgirpc.schema.ProtocolVersion;

/**
 * {@code vgi.attach_tickets.v1}: sealing a user's ATTACH so a runner holding their grant can
 * replay it later ({@code docs/protocol/vgi-attach-tickets.md}).
 *
 * <p>The framework hosts it beside {@code vgi.v2}, on HTTP only, and only when the worker's
 * signing key is configured explicitly and the worker can issue grants; see
 * {@link Worker#signingKey(byte[])}. Absent otherwise, so a client learns the answer from
 * reflection.
 */
@ProtocolName(AttachTickets.PROTOCOL_NAME)
@ProtocolVersion(AttachTickets.PROTOCOL_VERSION)
public interface AttachTicketsService {

    /**
     * Seal the caller's attach of {@code request.catalog_name()} into a ticket.
     *
     * <p>The caller must be authenticated (anonymous is {@code action_denied}); the principal
     * sealed is the caller's. Options are checked against the catalog's declared attach options
     * ({@code invalid_request} with a {@code BadRequest}). No fresh login is required: a ticket
     * carries no authority.
     *
     * @param request what to seal
     * @param ctx the call context carrying the caller
     * @return the ticket and its expiry
     */
    AttachTicket seal_attach(SealAttachRequest request, CallContext ctx);
}
