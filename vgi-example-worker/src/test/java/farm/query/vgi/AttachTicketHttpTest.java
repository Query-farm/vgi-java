// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.example.Main;
import farm.query.vgi.example.table.TicketProbeFixture;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.protocol.AttachTicket;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.SealAttachRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpRpcStream;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.identity.GrantKeys;
import farm.query.vgirpc.identity.Identity;
import farm.query.vgirpc.identity.IssuedGrant;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attach tickets end to end over HTTP against the fixture worker's {@code ticket_probe} catalog
 * ({@code docs/protocol/vgi-attach-tickets.md}): a fresh-login user attaches with
 * {@code region} and the secret {@code api_key}, seals the attach and mints a grant; a runner
 * authenticated only by {@code Bearer <grant>} attaches with nothing but
 * {@code vgi_attach_ticket} and reads the same probe row; another principal's grant cannot use it.
 */
@Timeout(120)
final class AttachTicketHttpTest {

    private static final GrantKeys KEYS;
    private static final byte[] SIGNING_KEY = "attach-ticket-e2e-signing-key".getBytes(StandardCharsets.UTF_8);
    private static final String API_KEY = "sk-test-0123456789";

    static {
        byte[] k = new byte[32];
        for (int i = 0; i < 32; i++) k[i] = (byte) (0x20 + i);
        KEYS = new GrantKeys(List.of(k), "vgi-java-ticket-test", 3600);
    }

    private static HttpServer http;
    private static String url;

    @BeforeAll
    static void start() throws Exception {
        Worker worker = Main.buildWorker("example", null, null).signingKey(SIGNING_KEY).grantKeys(KEYS);
        RpcServer rpc = worker.buildServer(Worker.Transport.HTTP);
        http = new HttpServer(rpc, HttpServer.Config.builder().host("127.0.0.1").port(0)
                .authenticator(Main.optionalTestBearer(KEYS)).build());
        http.start();
        url = "http://127.0.0.1:" + http.port();
    }

    @AfterAll
    static void stop() throws Exception {
        if (http != null) http.stop();
    }

    private static HttpRpcConnection as(String bearer) {
        HttpRpcConnection.Builder b = HttpRpcConnection.builder(url);
        if (bearer != null) b.bearerToken(bearer);
        return b.build();
    }

    @Test
    void aRunnerHoldingOnlyAGrantReattachesWithTheTicket() throws Exception {
        String ticket;
        IssuedGrant grant;
        try (HttpRpcConnection alice = as("vgi-test-alice")) {
            VgiService vgi = alice.proxy(VgiService.class);
            byte[] options = options(Map.of("region", "eu-west-2", "api_key", API_KEY));
            byte[] handle = vgi.catalog_attach(
                    CatalogAttachRequest.of(TicketProbeFixture.CATALOG_NAME, options, "", ""), null)
                    .attach_opaque_data();
            assertEquals(List.of("eu-west-2", "0d3b56072291"), probe(vgi, handle));

            AttachTicket sealed = alice.proxy(AttachTicketsService.class).seal_attach(
                    new SealAttachRequest(TicketProbeFixture.CATALOG_NAME, options, "", "", 0), null);
            ticket = sealed.ticket();
            assertTrue(ticket.startsWith(AttachTickets.PREFIX), ticket);
            long now = System.currentTimeMillis() / 1000;
            assertTrue(Math.abs(sealed.expires_at() - (now + KEYS.maxTtlSeconds())) <= 5,
                    "ttl 0 means the grant maximum: " + sealed.expires_at());
            grant = issueGrant(alice, "nightly");
        }

        // The runner: authenticated only by the grant, presenting only the ticket, under a name
        // the sealed one overrides.
        try (HttpRpcConnection runner = as(grant.token())) {
            VgiService vgi = runner.proxy(VgiService.class);
            byte[] handle = vgi.catalog_attach(CatalogAttachRequest.of("example",
                    options(Map.of(AttachTickets.OPTION, ticket)), "", ""), null).attach_opaque_data();
            assertEquals(List.of("eu-west-2", "0d3b56072291"), probe(vgi, handle),
                    "the secret took effect without travelling again");

            RpcError extra = assertThrows(RpcError.class, () -> vgi.catalog_attach(CatalogAttachRequest.of(
                    TicketProbeFixture.CATALOG_NAME,
                    options(Map.of(AttachTickets.OPTION, ticket, "region", "us-west-1")), "", ""), null));
            assertEquals("invalid_request", extra.errorKind(), extra.toString());
            assertFalse(extra.getMessage().contains(ticket.substring(6, 30)), "the ticket is never echoed");
        }

        // Bob's grant (and bob's own login) cannot open alice's ticket.
        IssuedGrant bobGrant;
        try (HttpRpcConnection bob = as("vgi-test-bob")) {
            bobGrant = issueGrant(bob, "nightly");
            assertTicketRefused(bob, ticket, "attach_ticket_invalid");
        }
        try (HttpRpcConnection bobRunner = as(bobGrant.token())) {
            assertTicketRefused(bobRunner, ticket, "attach_ticket_invalid");
        }
        try (HttpRpcConnection anonymous = as(null)) {
            assertTicketRefused(anonymous, ticket, "attach_ticket_invalid");
        }
    }

    @Test
    void sealAttachValidatesTheCallerAndTheOptions() {
        try (HttpRpcConnection anonymous = as(null)) {
            RpcError e = assertThrows(RpcError.class, () -> anonymous.proxy(AttachTicketsService.class)
                    .seal_attach(new SealAttachRequest(TicketProbeFixture.CATALOG_NAME,
                            options(Map.of("api_key", API_KEY)), "", "", 0), null));
            assertEquals("action_denied", e.errorKind(), e.toString());
        }
        try (HttpRpcConnection alice = as("vgi-test-alice")) {
            AttachTicketsService tickets = alice.proxy(AttachTicketsService.class);
            RpcError missing = assertThrows(RpcError.class, () -> tickets.seal_attach(new SealAttachRequest(
                    TicketProbeFixture.CATALOG_NAME, options(Map.of("region", "x")), "", "", 0), null));
            assertEquals("invalid_request", missing.errorKind());
            assertTrue(missing.toString().contains("options.api_key") || missing.errorDetails().toString()
                    .contains("options.api_key"), missing.errorDetails().toString());
            RpcError unknown = assertThrows(RpcError.class, () -> tickets.seal_attach(new SealAttachRequest(
                    "no_such_catalog", null, "", "", 0), null));
            assertEquals("invalid_request", unknown.errorKind());
            RpcError nested = assertThrows(RpcError.class, () -> tickets.seal_attach(new SealAttachRequest(
                    TicketProbeFixture.CATALOG_NAME,
                    options(Map.of("api_key", API_KEY, "VGI_ATTACH_TICKET", "x")), "", "", -1), null));
            assertEquals("invalid_request", nested.errorKind());
            String details = nested.errorDetails().toString();
            assertTrue(details.contains("options.VGI_ATTACH_TICKET") && details.contains("ttl_seconds"), details);
            AttachTicket capped = tickets.seal_attach(new SealAttachRequest(TicketProbeFixture.CATALOG_NAME,
                    options(Map.of("api_key", API_KEY)), "", "", 60), null);
            assertTrue(capped.expires_at() - System.currentTimeMillis() / 1000.0 <= 61, "a requested ttl is honoured");
        }
    }

    @Test
    void theProtocolIsHostedOnlyWithAConfiguredKeyGrantsAndHttp() {
        Assumptions.assumeTrue(System.getenv(Worker.SIGNING_KEY_ENV) == null
                && System.getenv(GrantKeys.KEYS_ENV) == null, "the environment configures keys");
        assertTrue(hosts(Worker.builder().catalogName("t").signingKey(SIGNING_KEY).grantKeys(KEYS),
                Worker.Transport.HTTP));
        assertTrue(hosts(Worker.builder().catalogName("t").signingKey(SIGNING_KEY)
                .mintGrant((principal, purpose, scopes, ttl) -> { throw new UnsupportedOperationException(); }),
                Worker.Transport.HTTP), "a worker minting its own grants can issue them");
        assertFalse(hosts(Worker.builder().catalogName("t").grantKeys(KEYS), Worker.Transport.HTTP),
                "no explicit key: a per-process key would kill every ticket on restart");
        assertFalse(hosts(Worker.builder().catalogName("t").signingKey(SIGNING_KEY), Worker.Transport.HTTP),
                "no way to issue a grant");
        for (Worker.Transport t : List.of(Worker.Transport.PIPE, Worker.Transport.UNIX, Worker.Transport.TCP)) {
            assertFalse(hosts(Worker.builder().catalogName("t").signingKey(SIGNING_KEY).grantKeys(KEYS), t),
                    "HTTP only: " + t);
        }
    }

    @Test
    void theProbeAttachNeverHoldsTheKey() {
        // On stdio the attach id travels unsealed: it holds region and digest, never the key.
        Worker w = Main.buildWorker("example", null, null);
        farm.query.vgi.internal.VgiServiceImpl svc = new farm.query.vgi.internal.VgiServiceImpl(
                w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
        byte[] id = svc.catalog_attach(CatalogAttachRequest.of(TicketProbeFixture.CATALOG_NAME,
                options(Map.of("region", "eu-west-2", "api_key", API_KEY)), "", ""), null).attach_opaque_data();
        String text = new String(id, StandardCharsets.ISO_8859_1);
        assertFalse(text.contains(API_KEY), "the key is in the attach id");
        assertTrue(text.contains("eu-west-2") && text.contains("0d3b56072291"), text);
    }

    private static boolean hosts(Worker worker, Worker.Transport transport) {
        return worker.buildServer(transport).applicationProtocols().stream()
                .anyMatch(p -> p.name().equals(AttachTickets.PROTOCOL_NAME));
    }

    private static void assertTicketRefused(HttpRpcConnection conn, String ticket, String kind) {
        RpcError e = assertThrows(RpcError.class, () -> conn.proxy(VgiService.class).catalog_attach(
                CatalogAttachRequest.of(TicketProbeFixture.CATALOG_NAME,
                        options(Map.of(AttachTickets.OPTION, ticket)), "", ""), null));
        assertEquals(kind, e.errorKind(), e.toString());
    }

    /** A one-row record of utf8 options. */
    private static byte[] options(Map<String, String> options) {
        Map<String, String> ordered = new LinkedHashMap<>(options);
        List<Field> fields = new ArrayList<>();
        for (String name : ordered.keySet()) fields.add(new Field(name, FieldType.nullable(new ArrowType.Utf8()), null));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), Allocators.root())) {
            root.allocateNew();
            int i = 0;
            for (String value : ordered.values()) {
                ((VarCharVector) root.getVector(i++)).setSafe(0, value.getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    /** Scan {@code main.ticket_probe} under {@code handle}: its one row, region then digest. */
    private static List<String> probe(VgiService vgi, byte[] handle) {
        BindRequest bind = new BindRequest(
                "ticket_probe", ArgumentsEncoder.positionalArgs(), "TABLE",
                null, SettingsEncoder.builder().encode(), null,
                handle, null, false,
                null, null, null, null,
                "main");
        BindResponse bound = vgi.bind(bind, null);
        InitRequest init = new InitRequest(
                RecordCodec.serializeToBytes(bind), bound.output_schema(), bound.opaque_data(),
                null, null, null, null, null, null,
                null, null, null, null, null, null, null, null,
                null, null);
        RpcStream<? extends StreamState> stream = vgi.init(init, null);
        List<String> row = new ArrayList<>();
        HttpRpcStream<?> session = (HttpRpcStream<?>) stream;
        try {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = session.tick();
                } catch (NoSuchElementException end) {
                    break;
                }
                VectorSchemaRoot root = batch.root();
                for (int i = 0; i < root.getRowCount(); i++) {
                    row.add(new String(((VarCharVector) root.getVector("region")).get(i), StandardCharsets.UTF_8));
                    row.add(new String(((VarCharVector) root.getVector("api_key_sha256")).get(i), StandardCharsets.UTF_8));
                }
            }
        } finally {
            session.close();
        }
        return row;
    }

    private static IssuedGrant issueGrant(HttpRpcConnection conn, String purpose) throws Exception {
        var params = ServiceIntrospector.describe(Identity.class).get("issue_grant").paramsSchema();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("purpose", purpose);
        row.put("scopes", List.of("read"));
        row.put("ttl_seconds", 600L);
        byte[] reply;
        try (VectorSchemaRoot root = Marshalling.encodeRow(params, row, Allocators.root())) {
            reply = conn.callUnaryRaw(Identity.PROTOCOL_NAME, "", "issue_grant", new AnnotatedBatch(root, Map.of()));
        }
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(reply), Allocators.root())) {
            r.readNextBatch();
            byte[] bytes = ((VarBinaryVector) r.root().getVector("result")).get(0);
            return RecordCodec.deserializeFromBytes(bytes, IssuedGrant.class);
        }
    }
}
