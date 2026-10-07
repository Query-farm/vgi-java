// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Result of {@code vgi.attach_tickets.v1} {@code seal_attach}
 * ({@code docs/protocol/vgi-attach-tickets.md} §5.2).
 *
 * @param ticket the {@code vgia1.} text; not a credential, but never logged
 * @param expires_at Unix seconds after which the worker refuses it; {@code +Infinity} when the
 *        worker sets no maximum lifetime
 */
public record AttachTicket(String ticket, double expires_at) implements ArrowSerializableRecord {
}
