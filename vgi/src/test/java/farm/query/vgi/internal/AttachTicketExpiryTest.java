// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachTickets;
import farm.query.vgi.Worker;
import farm.query.vgi.protocol.AttachTicket;
import farm.query.vgi.protocol.SealAttachRequest;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code vgi.attach_tickets.v1} protocol {@code 2.0.0}: {@code AttachTicket.expires_at} is a
 * nullable {@code timestamp[us, tz="UTC"]}, null meaning no expiry. Protocol {@code 1.0.0}
 * carried a {@code float64} of unix seconds with {@code +Infinity} for "no expiry".
 */
class AttachTicketExpiryTest {

    private static final byte[] SIGNING_KEY = new byte[32];

    private static CallContext alice() {
        return new CallContext(new AuthContext("test", true, "alice", Map.of()),
                null, Map.of(), "test", "seal_attach", AttachTickets.PROTOCOL_NAME, "r1", null);
    }

    private static AttachTicket seal(Long maxTtlSeconds, long ttlSeconds) {
        Worker w = Worker.builder().catalogName("ticket_catalog");
        return new AttachTicketsImpl(w, SIGNING_KEY, maxTtlSeconds).seal_attach(
                new SealAttachRequest("ticket_catalog", null, "", "", ttlSeconds), alice());
    }

    @Test
    void protocolVersionIsTwo() {
        assertEquals("2.0.0", AttachTickets.PROTOCOL_VERSION);
    }

    @Test
    void wireTypeIsANullableUtcMicrosecondTimestamp() {
        Field f = farm.query.vgirpc.schema.SchemaDerivation.schemaForRecord(AttachTicket.class)
                .findField("expires_at");
        assertEquals(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), f.getType());
        assertTrue(f.isNullable());
    }

    @Test
    void noMaximumLifetimeIsNullOnTheWire() {
        AttachTicket t = seal(null, 0);
        assertNull(t.expires_at());
        try (VectorSchemaRoot root = BatchUtil.readSingleBatch(RecordCodec.serializeToBytes(t), Allocators.root())) {
            assertTrue(root.getVector("expires_at").isNull(0));
        }
        assertNull(RecordCodec.deserializeFromBytes(RecordCodec.serializeToBytes(t), AttachTicket.class)
                .expires_at());
    }

    @Test
    void aRequestedTtlWithNoCeilingIsHonoured() {
        long before = System.currentTimeMillis() / 1000;
        AttachTicket t = seal(null, 120);
        assertNotNull(t.expires_at());
        long seconds = t.expires_at().getEpochSecond();
        assertTrue(seconds >= before + 120 && seconds <= before + 122, t.expires_at().toString());
    }

    @Test
    void aLifetimeIsTheSealedExpiryAsMicroseconds() {
        long before = System.currentTimeMillis() / 1000;
        AttachTicket t = seal(3600L, 60);
        assertNotNull(t.expires_at());
        long seconds = t.expires_at().getEpochSecond();
        assertTrue(seconds >= before + 60 && seconds <= before + 62, t.expires_at().toString());
        assertEquals(0, t.expires_at().getNano(), "the sealed claim is whole seconds");
        // The value on the wire is the same instant, in microseconds since the epoch.
        try (VectorSchemaRoot root = BatchUtil.readSingleBatch(RecordCodec.serializeToBytes(t), Allocators.root())) {
            TimeStampMicroTZVector v = (TimeStampMicroTZVector) root.getVector("expires_at");
            assertEquals("UTC", v.getTimeZone());
            assertEquals(seconds * 1_000_000L, v.get(0));
        }
        // ...and the sealed claims agree with it.
        AttachTickets.Claims claims = AttachTickets.open(SIGNING_KEY, t.ticket(), "alice", before);
        assertEquals(seconds, claims.expiresAt());
    }

    @Test
    void roundTripsAnInstant() {
        Instant at = Instant.parse("2030-01-01T00:00:00Z");
        AttachTicket back = RecordCodec.deserializeFromBytes(
                RecordCodec.serializeToBytes(new AttachTicket("vgia1.x", at)), AttachTicket.class);
        assertEquals(at, back.expires_at());
    }
}
