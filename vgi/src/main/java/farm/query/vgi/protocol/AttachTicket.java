// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.time.Instant;

/**
 * Result of {@code vgi.attach_tickets.v1} {@code seal_attach}
 * ({@code docs/protocol/vgi-attach-tickets.md} §5.2).
 *
 * <p>Protocol {@code 2.0.0} carries {@code expires_at} as a nullable
 * {@code timestamp[us, tz="UTC"]}; {@code 1.0.0} carried a {@code float64} of unix seconds
 * with {@code +Infinity} for "no expiry".
 *
 * @param ticket the {@code vgia1.} text; not a credential, but never logged
 * @param expires_at the instant after which the worker refuses it; {@code null} when the
 *        worker sets no maximum lifetime
 */
public record AttachTicket(String ticket, @Nullable Instant expires_at) implements ArrowSerializableRecord {
}
