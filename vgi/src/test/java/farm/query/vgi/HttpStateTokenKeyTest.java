// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.http.HttpServer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The HTTP stream-state token key follows the configured signing key, so
 * table-function streams resume across replicas and restarts. Before, {@code --http}
 * and {@link Worker#runHttp} never set a token key and vgi-rpc sealed every state
 * token with a random per-process key.
 */
class HttpStateTokenKeyTest {

    private static final byte[] SIGNING = "replica-shared-signing-key-0123456789"
            .getBytes(StandardCharsets.UTF_8);

    @Test
    void derivationIsHkdfSha256WithTheStateTokenLabel() {
        // RFC 5869 HKDF-SHA256, salt = 32 zero bytes, info = label, L = 32.
        // Pinned so a refactor can't silently change the key every replica derives.
        byte[] ikm = new byte[32]; // all-zero key
        assertEquals("577841cbcde49fa70aba6d6f603ea613775b2b41e50a7a7ab747cf7d50da33de",
                HexFormat.of().formatHex(Worker.deriveStateTokenKey(ikm)));
        byte[] a = Worker.deriveStateTokenKey(SIGNING);
        byte[] b = Worker.deriveStateTokenKey(SIGNING.clone());
        assertEquals(32, a.length);
        assertArrayEquals(a, b, "deterministic: every replica derives the same key");
        assertFalse(java.util.Arrays.equals(a, java.util.Arrays.copyOf(SIGNING, 32)),
                "never the raw signing key");
        assertFalse(java.util.Arrays.equals(a, Worker.deriveStateTokenKey(new byte[32])));
    }

    @Test
    void signingKeyDerivesTheTokenKey() {
        HttpServer.Config cfg = HttpServer.Config.builder().host("127.0.0.1").port(0).build();
        assertNull(cfg.tokenKey());
        HttpServer.Config out = Worker.withStateTokenKey(cfg, SIGNING, null);
        assertArrayEquals(Worker.deriveStateTokenKey(SIGNING), out.tokenKey());
        // Everything else is carried over.
        assertEquals(cfg.host(), out.host());
        assertEquals(cfg.port(), out.port());
        assertEquals(cfg.prefix(), out.prefix());
        assertEquals(cfg.tokenTtlSeconds(), out.tokenTtlSeconds());
        assertEquals(cfg.maxRequestBytes(), out.maxRequestBytes());
    }

    @Test
    void explicitTokenKeyWins() {
        byte[] explicit = new byte[32];
        explicit[0] = 7;
        HttpServer.Config cfg = HttpServer.Config.builder().tokenKey(explicit).build();
        assertSame(cfg, Worker.withStateTokenKey(cfg, SIGNING, null));
    }

    @Test
    void noSigningKeyKeepsThePerProcessKey() {
        HttpServer.Config cfg = HttpServer.Config.builder().build();
        assertSame(cfg, Worker.withStateTokenKey(cfg, null, null));
    }
}
