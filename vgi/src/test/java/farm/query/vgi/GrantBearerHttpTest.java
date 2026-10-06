// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallStatistics;
import farm.query.vgirpc.DispatchHook;
import farm.query.vgirpc.DispatchInfo;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.http.InvalidCredentials;
import farm.query.vgirpc.identity.GrantKeys;
import farm.query.vgirpc.identity.Identity;
import farm.query.vgirpc.identity.IssuedGrant;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The grant loop, closed end to end over HTTP: a freshly logged-in user mints a grant through
 * {@code issue_grant}, and a {@code vgi.v2} call presenting it as {@code Bearer <grant>} is
 * authenticated as that user.
 */
@Timeout(60)
final class GrantBearerHttpTest {

    private static final GrantKeys KEYS;

    static {
        byte[] k = new byte[32];
        for (int i = 0; i < 32; i++) k[i] = (byte) (0x40 + i);
        KEYS = new GrantKeys(List.of(k), "vgi-java-test", 3600);
    }

    /** Fresh user login: {@code X-Test-Principal} with a current auth_time; a bearer is not ours. */
    private static final Authenticator LOGIN = req -> {
        String p = req.getHeader("X-Test-Principal");
        if (p != null) {
            return new AuthContext("login", true, p, Map.of("auth_time", System.currentTimeMillis() / 1000.0));
        }
        if (req.getHeader("Authorization") != null) throw new InvalidCredentials("not mine");
        return AuthContext.ANONYMOUS;
    };

    /** Records who each vgi.v2 dispatch was authenticated as. */
    private static final class Who implements DispatchHook {
        final List<String> seen = new CopyOnWriteArrayList<>();

        @Override public Object onDispatchStart(DispatchInfo info) {
            if (VgiService.PROTOCOL_NAME.equals(info.protocol)) seen.add(info.principal + "|" + info.authDomain);
            return null;
        }

        @Override public void onDispatchEnd(Object token, DispatchInfo info, CallStatistics stats, Throwable error) { }
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

    @Test
    void aMintedGrantAuthenticatesAVgiCallAsItsOwner() throws Exception {
        Worker worker = Worker.builder().catalogName("grant_probe").grantKeys(KEYS);
        RpcServer rpc = worker.buildServer(Worker.Transport.HTTP);
        assertEquals(java.util.Set.of("issue_grant"), rpc.identityMethodTable().keySet(),
                "keys alone host the sealed minter");
        Who who = new Who();
        rpc.setDispatchHook(who);
        HttpServer http = new HttpServer(rpc, HttpServer.Config.builder().prefix("/vgi").authenticator(LOGIN).build());
        http.start();
        String url = "http://127.0.0.1:" + http.port() + "/vgi";
        try {
            IssuedGrant grant;
            try (HttpRpcConnection alice = HttpRpcConnection.builder(url).header("X-Test-Principal", "alice").build()) {
                grant = issueGrant(alice, "nightly");
            }
            assertTrue(grant.token().startsWith("vgig1."), grant.token());
            try (HttpRpcConnection bot = HttpRpcConnection.builder(url).bearerToken(grant.token()).build()) {
                bot.proxy(VgiService.class).catalog_catalogs();
                assertEquals(List.of("alice|grant"), who.seen, "the vgi.v2 call is authenticated as the owner");
                RpcError e = assertThrows(RpcError.class, () -> issueGrant(bot, "child"));
                assertEquals("stale_auth", e.errorKind(), "a grant cannot mint a grant");
            }
            try (HttpRpcConnection forged = HttpRpcConnection.builder(url).bearerToken(grant.token() + "x").build()) {
                assertEquals("AuthenticationError",
                        assertThrows(RpcError.class, () -> forged.proxy(VgiService.class).catalog_catalogs()).errorType());
            }
        } finally {
            http.stop();
        }
    }

    @Test
    void grantsAreHttpOnly() {
        Worker worker = Worker.builder().catalogName("grant_probe").grantKeys(KEYS);
        assertNull(worker.buildServer(Worker.Transport.PIPE).identity());
        assertNull(worker.buildServer(Worker.Transport.UNIX).identity());
        assertNull(worker.buildServer(Worker.Transport.TCP).identity());
    }

    @Test
    void theAllowlistRuleStillHoldsWithGrants() {
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("VGI_INTROSPECT_PRINCIPALS") == null);
        Worker worker = Worker.builder().catalogName("grant_probe").grantKeys(KEYS).resolveToken(t -> null);
        assertThrows(IllegalArgumentException.class, () -> worker.buildServer(Worker.Transport.HTTP));
    }
}
