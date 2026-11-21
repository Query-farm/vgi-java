// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.HttpServer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * In-process checks of {@link OrchardHosted}'s environment wiring and {@link Worker}'s hosted
 * configuration. The end-to-end contract is {@code OrchardHostedProcessTest} in the example worker.
 */
final class OrchardHostedTest {

    private static final String JWKS = "http://127.0.0.1:9/jwks";

    private static Map<String, String> hostedEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("VGI_ORCHARD_HOSTED", "1");
        env.put("VGI_JWT_ISSUER", "https://auth.orchard.test");
        env.put("VGI_JWT_AUDIENCE", "https://desk.orchard.test/d/dep_123");
        env.put("VGI_JWT_JWKS_URI", JWKS);
        env.put("VGI_SIGNING_KEY", "orchard-hosted-test-signing-key-0001");
        return env;
    }

    @Test
    void switchValues() {
        for (String on : List.of("1", "true", "YES", " on ")) {
            assertTrue(OrchardHosted.enabled(Map.of("VGI_ORCHARD_HOSTED", on)), on);
        }
        for (String off : List.of("0", "false", "", "off", "No")) {
            assertFalse(OrchardHosted.enabled(Map.of("VGI_ORCHARD_HOSTED", off)), off);
        }
        assertFalse(OrchardHosted.enabled(Map.of()));
        OrchardHosted.RefusedException e = assertThrows(OrchardHosted.RefusedException.class,
                () -> OrchardHosted.enabled(Map.of("VGI_ORCHARD_HOSTED", "yes please")));
        assertTrue(e.getMessage().contains("VGI_ORCHARD_HOSTED"));
    }

    @Test
    void resolveConfigDefaultsAndOverrides() {
        OrchardHosted.Config c = OrchardHosted.resolveConfig(hostedEnv());
        assertEquals(List.of("ES256"), c.algorithms());
        Map<String, String> env = hostedEnv();
        env.put("VGI_JWT_ALGORITHMS", "ES256, ES384");
        assertEquals(List.of("ES256", "ES384"), OrchardHosted.resolveConfig(env).algorithms());
        for (String bad : List.of("ES256,HS256", "none", " , ")) {
            env.put("VGI_JWT_ALGORITHMS", bad);
            assertThrows(OrchardHosted.RefusedException.class, () -> OrchardHosted.resolveConfig(env), bad);
        }
    }

    @Test
    void hostedConfigInstallsThePassVerifierAndStrictAuth() {
        HttpServer.Config hosted = Worker.builder().orchardHosted(HttpServer.Config.builder().build(), hostedEnv());
        assertTrue(OrchardHosted.isPassAuthenticator(hosted.authenticator()));
        assertTrue(hosted.strictAuth());
        assertEquals(OrchardHosted.CHALLENGE, hosted.wwwAuthenticate());
        assertEquals(List.of("*"), hosted.corsOrigins());
    }

    @Test
    void customAuthenticatorRefused() {
        HttpServer.Config custom = HttpServer.Config.builder()
                .authenticator(request -> AuthContext.ANONYMOUS).build();
        OrchardHosted.RefusedException e = assertThrows(OrchardHosted.RefusedException.class,
                () -> Worker.builder().orchardHosted(custom, hostedEnv()));
        assertTrue(e.getMessage().contains("authenticator"), e.getMessage());
    }

    @Test
    void identityHooksAndGrantKeysRefused() {
        assertThrows(OrchardHosted.RefusedException.class, () -> Worker.builder()
                .resolveToken(token -> null)
                .orchardHosted(HttpServer.Config.builder().build(), hostedEnv()));
        assertThrows(OrchardHosted.RefusedException.class, () -> Worker.builder()
                .mintGrant((principal, purpose, scopes, ttl) -> null)
                .orchardHosted(HttpServer.Config.builder().build(), hostedEnv()));
    }

    @Test
    void signingKeyInCodeCounts() {
        Map<String, String> env = hostedEnv();
        env.remove("VGI_SIGNING_KEY");
        assertThrows(OrchardHosted.RefusedException.class,
                () -> Worker.builder().orchardHosted(HttpServer.Config.builder().build(), env));
        Worker.builder().signingKey(new byte[32]).orchardHosted(HttpServer.Config.builder().build(), env);
        assertThrows(OrchardHosted.RefusedException.class, () -> Worker.builder().signingKey(new byte[31])
                .orchardHosted(HttpServer.Config.builder().build(), env));
    }

    @Test
    void proxyAndPeerIdentityRefused() {
        assertThrows(OrchardHosted.RefusedException.class, () -> Worker.builder().orchardHosted(
                HttpServer.Config.builder().proxyProofRequired(true).build(), hostedEnv()));
        assertThrows(OrchardHosted.RefusedException.class, () -> Worker.builder().orchardHosted(
                HttpServer.Config.builder().proxyAuthHeaders(List.of("x-forwarded-client-cert")).build(),
                hostedEnv()));
    }

    @Test
    void nonHttpTransportsRefused() {
        // runStdio/runUnixSocket/runTcp read the real environment; the refusal itself is checked here.
        assertTrue(OrchardHosted.transportRefusal("--tcp").getMessage().contains("only --http"));
    }
}
