// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.ErrorDetail;
import farm.query.vgirpc.errors.StatusError;

import java.util.List;

/**
 * An authentic attach ticket outside its lifetime (expired, or issued in the future beyond the
 * clock skew).
 *
 * <p>{@code FAILED_PRECONDITION} / kind {@code attach_ticket_expired}: the remedy is a fresh
 * {@code seal_attach} from a logged-in session, not a retry. Only raised once the ticket opened
 * under the caller's principal, so it tells a forger nothing.
 */
public final class AttachTicketExpiredError extends StatusError {

    /** The stable error kind. */
    public static final String KIND = "attach_ticket_expired";

    /**
     * Build the error.
     *
     * @param detail operator-facing reason; never the ticket text
     */
    public AttachTicketExpiredError(String detail) {
        super(detail, Code.FAILED_PRECONDITION, KIND, List.of(new ErrorDetail.PreconditionFailure(List.of(
                new ErrorDetail.PreconditionViolation("ATTACH_TICKET", AttachTickets.OPTION, detail)))));
    }
}
