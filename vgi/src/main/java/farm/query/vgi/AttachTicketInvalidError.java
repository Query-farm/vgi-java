// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.ErrorDetail;
import farm.query.vgirpc.errors.StatusError;

import java.util.List;

/**
 * A {@code vgi_attach_ticket} this worker cannot accept.
 *
 * <p>One type for every cause -- malformed, wrong prefix, non-canonical, wrong key, wrong
 * principal, tampered, bad payload -- so a caller cannot tell a forged ticket from another
 * user's. {@code INVALID_ARGUMENT} / kind {@code attach_ticket_invalid}, with a
 * {@code BadRequest} naming the {@code vgi_attach_ticket} field. The message never contains the
 * ticket.
 */
public final class AttachTicketInvalidError extends StatusError {

    /** The stable error kind. */
    public static final String KIND = "attach_ticket_invalid";

    /**
     * Build the error.
     *
     * @param detail operator-facing reason; never the ticket text
     */
    public AttachTicketInvalidError(String detail) {
        super(detail, Code.INVALID_ARGUMENT, KIND, List.of(new ErrorDetail.BadRequest(List.of(
                new ErrorDetail.FieldViolation(AttachTickets.OPTION, detail)))));
    }
}
