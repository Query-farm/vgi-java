// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.identity.Identity;
import farm.query.vgirpc.identity.TokenIdentity;
import farm.query.vgirpc.schema.ProtocolName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The hosting hook and the Identity opt-in, through the one server builder every transport uses.
 */
final class HostedProtocolsTest {

    @ProtocolName("demo.Extra.v1")
    public interface Extra {
        String ping();
    }

    @ProtocolName("vgi_rpc.Shadow.v1")
    public interface Shadow {
        String ping();
    }

    @ProtocolName("vgi.v2")
    public interface Impostor {
        String ping();
    }

    static final class ExtraImpl implements Extra, Shadow, Impostor {
        @Override public String ping() { return "pong"; }
    }

    private static Worker worker() {
        return Worker.builder().catalogName("hosted_probe");
    }

    @Test
    void theHookIsHostedAfterVgiOnEveryTransportAndCalledOncePerBuild() {
        AtomicInteger calls = new AtomicInteger();
        Worker w = worker().hostedProtocols(() -> {
            calls.incrementAndGet();
            return List.of(HostedProtocol.of(Extra.class, new ExtraImpl()));
        });
        for (Worker.Transport t : Worker.Transport.values()) {
            RpcServer server = w.buildServer(t);
            List<String> names = server.applicationProtocols().stream()
                    .map(RpcServer.ApplicationProtocol::name).toList();
            assertEquals(List.of("vgi.v2", "demo.Extra.v1"), names, t.name());
        }
        assertEquals(Worker.Transport.values().length, calls.get());
    }

    @Test
    void noHookHostsOnlyVgi() {
        assertEquals(1, worker().rpcServer().applicationProtocols().size());
    }

    @Test
    void validationErrorsNameTheHook() {
        IllegalArgumentException reserved = assertThrows(IllegalArgumentException.class, () -> worker()
                .hostedProtocols(() -> List.of(HostedProtocol.of(Shadow.class, new ExtraImpl())))
                .rpcServer());
        assertTrue(reserved.getMessage().contains("Worker.hostedProtocols"), reserved.getMessage());
        assertTrue(reserved.getMessage().contains("vgi_rpc."), reserved.getMessage());

        IllegalArgumentException own = assertThrows(IllegalArgumentException.class, () -> worker()
                .hostedProtocols(() -> List.of(HostedProtocol.of(Impostor.class, new ExtraImpl())))
                .rpcServer());
        assertTrue(own.getMessage().contains("Worker.hostedProtocols"), own.getMessage());

        IllegalArgumentException twice = assertThrows(IllegalArgumentException.class, () -> worker()
                .hostedProtocols(() -> List.of(HostedProtocol.of(Extra.class, new ExtraImpl()),
                        HostedProtocol.of(Extra.class, new ExtraImpl())))
                .rpcServer());
        assertTrue(twice.getMessage().contains("Worker.hostedProtocols"), twice.getMessage());
    }

    @Test
    void identityIsAbsentUnlessAHookIsSet() {
        assertNull(worker().buildServer(Worker.Transport.HTTP).identity());
    }

    @Test
    void identityIsHostedOnHttpOnly() {
        Worker w = worker()
                .resolveToken(token -> new TokenIdentity("p"))
                .introspectPrincipals("proxy");
        RpcServer http = w.buildServer(Worker.Transport.HTTP);
        assertNotNull(http.identity());
        assertEquals(java.util.Set.of("introspect_token"), http.identityMethodTable().keySet());
        assertNull(w.buildServer(Worker.Transport.PIPE).identity());
        assertNull(w.buildServer(Worker.Transport.UNIX).identity());
        assertNull(w.buildServer(Worker.Transport.TCP).identity());
        assertEquals(Identity.PROTOCOL_NAME, "vgi_rpc.Identity.v1");
    }

    @Test
    void introspectionWithoutAnAllowlistRefusesToStart() {
        // Skipped when the environment supplies one: the rule is "no allowlist anywhere".
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("VGI_INTROSPECT_PRINCIPALS") == null);
        Worker w = worker().resolveToken(token -> null);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> w.buildServer(Worker.Transport.HTTP));
        assertTrue(e.getMessage().contains("VGI_INTROSPECT_PRINCIPALS"), e.getMessage());
        // A worker that only mints needs no allowlist: minting is always about the caller.
        RpcServer minting = worker()
                .mintGrant((principal, purpose, scopes, ttl) -> null)
                .buildServer(Worker.Transport.HTTP);
        assertEquals(java.util.Set.of("issue_grant"), minting.identityMethodTable().keySet());
    }
}
