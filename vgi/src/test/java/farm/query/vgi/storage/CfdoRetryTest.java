// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Durable Object client's timeouts and retry policy: transport errors, 5xx
 * and 429 are retried with the same {@code attempt_id}; other 4xx are not; the
 * bearer token never reaches an exception message.
 */
class CfdoRetryTest {

    private static final String SHARD = "att-0123456789abcdef0123456789abcdef";
    private static final String TOKEN = "SECRET-TOKEN-do-not-log-4b1d";
    private static final CfdoStorage.Options FAST = new CfdoStorage.Options(
            Duration.ofSeconds(2), Duration.ofSeconds(5), 4, Duration.ofMillis(1), Duration.ofMillis(5));

    private MockDoServer mock;
    private CfdoStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        mock = new MockDoServer();
        storage = new CfdoStorage(mock.baseUrl(), TOKEN, FAST).forShard(SHARD);
    }

    @AfterEach
    void tearDown() {
        mock.close();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private void assertOneAttemptId(int expectedRequests) {
        assertEquals(expectedRequests, mock.requestLog.size(), mock.requestLog.toString());
        assertEquals(1, new HashSet<>(mock.requestLog).size(),
                "every retry carries the same attempt_id: " + mock.requestLog);
        assertFalse(mock.requestLog.get(0).endsWith(" "), "a destructive call has an attempt_id");
    }

    @Test
    void retries5xxWithSameAttemptId() {
        mock.faults.add(503);
        storage.statePut(b("e"), b("n"), b("k"), b("v"));
        assertOneAttemptId(2);
        assertArrayEquals(b("v"), storage.stateGet(b("e"), b("n"), b("k")));
    }

    @Test
    void retries429And500AndTransportErrorWithSameAttemptId() {
        mock.faults.add(429);
        mock.faults.add(0);
        mock.faults.add(500);
        long ordinal = storage.stateAppend(b("e"), b("buf"), b(""), b("v"));
        assertOneAttemptId(4);
        List<FunctionStorage.LogEntry> rows = storage.stateLogScan(b("e"), b("buf"), b(""), -1, 0);
        assertEquals(1, rows.size(), "applied exactly once");
        assertEquals(ordinal, rows.get(0).id());
    }

    @Test
    void readsRetryToo() {
        storage.statePut(b("e"), b("n"), b("k"), b("v"));
        mock.requestLog.clear();
        mock.faults.add(502);
        assertArrayEquals(b("v"), storage.stateGet(b("e"), b("n"), b("k")));
        assertEquals(2, mock.requestLog.size());
    }

    @Test
    void otherClientErrorsAreNotRetried() {
        for (int status : new int[] {400, 401, 404, 413}) {
            mock.requestLog.clear();
            mock.faults.add(status);
            CfdoStorage.CfdoException e = assertThrows(CfdoStorage.CfdoException.class,
                    () -> storage.statePut(b("e"), b("n"), b("k"), b("v")));
            assertTrue(e.getMessage().contains("error " + status), e.getMessage());
            assertEquals(1, mock.requestLog.size(), "status " + status + " must not be retried");
        }
    }

    @Test
    void exhaustedRetriesFailWithoutLeakingTheToken() {
        for (int i = 0; i < 4; i++) mock.faults.add(503);
        CfdoStorage.CfdoException e = assertThrows(CfdoStorage.CfdoException.class,
                () -> storage.statePut(b("e"), b("n"), b("k"), b("v")));
        assertTrue(e.getMessage().contains("failed after 4 attempt(s)"), e.getMessage());
        assertOneAttemptId(4);
        assertFalse(e.getMessage().contains(TOKEN), "the bearer token leaked into an exception");
        assertTrue(mock.authHeaders.stream().allMatch(h -> h.equals("Bearer " + TOKEN)));
    }

    @Test
    void requestTimeoutIsEnforcedAndRetried() throws IOException {
        HttpServer slow = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        java.util.concurrent.atomic.AtomicInteger hits = new java.util.concurrent.atomic.AtomicInteger();
        slow.createContext("/", ex -> {
            hits.incrementAndGet();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            ex.close();
        });
        slow.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        slow.start();
        try {
            CfdoStorage s = new CfdoStorage("http://127.0.0.1:" + slow.getAddress().getPort(), TOKEN,
                    new CfdoStorage.Options(Duration.ofSeconds(1), Duration.ofMillis(200), 2,
                            Duration.ofMillis(1), Duration.ofMillis(2))).forShard(SHARD);
            long t0 = System.nanoTime();
            CfdoStorage.CfdoException e = assertThrows(CfdoStorage.CfdoException.class,
                    () -> s.stateGet(b("e"), b("n"), b("k")));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 2500, "timed out after " + ms + " ms");
            assertTrue(e.getMessage().contains("Timeout"), e.getMessage());
            assertEquals(2, hits.get());
        } finally {
            slow.stop(0);
        }
    }

    @Test
    void optionsValidated() {
        assertThrows(IllegalArgumentException.class, () -> new CfdoStorage.Options(
                Duration.ZERO, Duration.ofSeconds(1), 1, Duration.ZERO, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new CfdoStorage.Options(
                Duration.ofSeconds(1), Duration.ofSeconds(1), 0, Duration.ZERO, Duration.ZERO));
        assertEquals(4, CfdoStorage.Options.defaults().maxAttempts());
    }
}
