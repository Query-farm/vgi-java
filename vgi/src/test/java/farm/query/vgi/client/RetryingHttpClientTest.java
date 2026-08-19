// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wrapper around the decision: that a refusal really is re-sent, that the
 * bounds really do end in an error naming what was tried, and that everything
 * else is passed through untouched.
 *
 * <p>Driven by a scripted delegate rather than a server, and with the jitter
 * source pinned to zero so the retries take no wall-clock time. What is being
 * tested here is the plumbing — attempt counting, body handling, which
 * responses are intercepted — while {@link RetryPolicyTest} covers the timing
 * arithmetic.</p>
 */
class RetryingHttpClientTest {

    private static final URI ENDPOINT = URI.create("http://worker.example/table_function_init");
    private static final String ARROW = "application/vnd.apache.arrow.stream";

    /** An Arrow IPC stream opens with the continuation marker then a metadata length. */
    private static final byte[] ARROW_PREFIX = {
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x10, 0x00, 0x00, 0x00};

    private static RetryingHttpClient client(RetryPolicy policy, ScriptedClient delegate) {
        // Jitter pinned to 0: with no Retry-After that makes every computed
        // delay zero, so the test exercises the retry loop without sleeping.
        return RetryingHttpClient.wrap(delegate, policy, Instant::now, () -> 0.0);
    }

    private static HttpRequest request() {
        return HttpRequest.newBuilder(ENDPOINT).POST(HttpRequest.BodyPublishers.noBody()).build();
    }

    @Test
    void resendsUntilTheWorkerHasCapacity() throws Exception {
        ScriptedClient delegate = new ScriptedClient(List.of(
                response(429, Map.of("retry-after", "0"), new byte[0]),
                response(429, Map.of("retry-after", "0"), new byte[0]),
                response(200, Map.of("content-type", ARROW), ARROW_PREFIX)));

        HttpResponse<byte[]> got = client(RetryPolicy.defaults(), delegate)
                .send(request(), HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(200, got.statusCode());
        assertEquals(3, delegate.sent.get(), "the two refusals should have been re-sent");
    }

    @Test
    void givingUpNamesTheEndpointAndTheAttemptCount() {
        RetryPolicy threeTries = RetryPolicy.of(3, Duration.ofSeconds(60),
                Duration.ofMillis(1), Duration.ofMillis(1));
        ScriptedClient delegate = ScriptedClient.always(
                response(429, Map.of("retry-after", "0"), new byte[0]));

        RetryBudgetExhaustedException e = assertThrows(RetryBudgetExhaustedException.class,
                () -> client(threeTries, delegate).send(request(), HttpResponse.BodyHandlers.ofByteArray()));

        assertEquals(3, delegate.sent.get());
        assertEquals(3, e.attempts());
        assertEquals(429, e.lastStatus());
        assertTrue(e.getMessage().contains(ENDPOINT.toString()), e.getMessage());
        assertTrue(e.getMessage().contains("3 attempts"), e.getMessage());
        assertTrue(e.getMessage().contains("Retry-After: 0"), e.getMessage());
    }

    @Test
    void aWorkerErrorBatchIsNotRetriedAndNotIntercepted() throws Exception {
        // 500 + an Arrow body is how a VGI HTTP server reports that the call
        // ran and threw. Retrying re-runs the failure; rewriting the error
        // loses the worker's own message.
        HttpResponse<byte[]> workerError = response(500, Map.of("content-type", ARROW), ARROW_PREFIX);
        ScriptedClient delegate = ScriptedClient.always(workerError);

        HttpResponse<byte[]> got = client(RetryPolicy.defaults(), delegate)
                .send(request(), HttpResponse.BodyHandlers.ofByteArray());

        assertSame(workerError, got);
        assertEquals(1, delegate.sent.get());
    }

    @Test
    void anAuthFailurePassesThroughForTheRpcLayerToClassify() throws Exception {
        HttpResponse<byte[]> unauthorized = response(401, Map.of("content-type", "application/json"),
                "{\"error\":\"unauthorized\"}".getBytes(StandardCharsets.UTF_8));
        ScriptedClient delegate = ScriptedClient.always(unauthorized);

        assertSame(unauthorized, client(RetryPolicy.defaults(), delegate)
                .send(request(), HttpResponse.BodyHandlers.ofByteArray()));
    }

    @Test
    void anEmptyBodyClaimingArrowFailsAboutTheStatusNotTheFraming() {
        // The failure this prevents: a gateway answers with the Arrow content
        // type and nothing in the body, the RPC layer trusts the type, and the
        // operator is told "empty IPC stream (no schema)" for what was a 413.
        ScriptedClient delegate = ScriptedClient.always(
                response(413, Map.of("content-type", ARROW), new byte[0]));

        IOException e = assertThrows(IOException.class,
                () -> client(RetryPolicy.defaults(), delegate)
                        .send(request(), HttpResponse.BodyHandlers.ofByteArray()));

        assertTrue(e.getMessage().contains("413"), e.getMessage());
        assertTrue(e.getMessage().contains("not an Arrow stream"), e.getMessage());
    }

    @Test
    void aProxyErrorPageClaimingArrowIsReportedWithItsText() {
        ScriptedClient delegate = ScriptedClient.always(response(451,
                Map.of("content-type", ARROW),
                "<html>blocked by policy</html>".getBytes(StandardCharsets.UTF_8)));

        IOException e = assertThrows(IOException.class,
                () -> client(RetryPolicy.defaults(), delegate)
                        .send(request(), HttpResponse.BodyHandlers.ofByteArray()));

        assertTrue(e.getMessage().contains("blocked by policy"), e.getMessage());
    }

    @Test
    void aCompressedErrorBodyIsLeftAloneRatherThanMisreadAsFraming() throws Exception {
        // Still-compressed bytes would fail the Arrow framing check for a
        // reason that has nothing to do with Arrow, so the check stands down.
        HttpResponse<byte[]> gzipped = response(400,
                Map.of("content-type", ARROW, "content-encoding", "gzip"),
                new byte[] {0x1f, (byte) 0x8b, 0x08, 0x00});

        assertSame(gzipped, client(RetryPolicy.defaults(), ScriptedClient.always(gzipped))
                .send(request(), HttpResponse.BodyHandlers.ofByteArray()));
    }

    @Test
    void theAsyncPathObeysTheSamePolicy() throws Exception {
        ScriptedClient delegate = new ScriptedClient(List.of(
                response(503, Map.of(), new byte[0]),
                response(200, Map.of("content-type", ARROW), ARROW_PREFIX)));

        HttpResponse<byte[]> got = client(RetryPolicy.defaults(), delegate)
                .sendAsync(request(), HttpResponse.BodyHandlers.ofByteArray())
                .get();

        assertEquals(200, got.statusCode());
        assertEquals(2, delegate.sent.get());
    }

    @Test
    void theAsyncPathFailsWithTheSameError() {
        RetryPolicy twoTries = RetryPolicy.of(2, Duration.ofSeconds(60),
                Duration.ofMillis(1), Duration.ofMillis(1));
        ScriptedClient delegate = ScriptedClient.always(response(429, Map.of(), new byte[0]));

        Exception e = assertThrows(Exception.class,
                () -> client(twoTries, delegate)
                        .sendAsync(request(), HttpResponse.BodyHandlers.ofByteArray()).get());

        assertInstanceOf(RetryBudgetExhaustedException.class, e.getCause());
        assertEquals(2, delegate.sent.get());
    }

    // ------------------------------------------------------------------
    // Test doubles
    // ------------------------------------------------------------------

    private static HttpResponse<byte[]> response(int status, Map<String, String> headers,
                                                 byte[] body) {
        HttpHeaders parsed = HttpHeaders.of(
                headers.entrySet().stream().collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey, e -> List.of(e.getValue()))),
                (k, v) -> true);
        return new HttpResponse<>() {
            @Override public int statusCode() { return status; }
            @Override public HttpRequest request() { return RetryingHttpClientTest.request(); }
            @Override public Optional<HttpResponse<byte[]>> previousResponse() { return Optional.empty(); }
            @Override public HttpHeaders headers() { return parsed; }
            @Override public byte[] body() { return body; }
            @Override public Optional<SSLSession> sslSession() { return Optional.empty(); }
            @Override public URI uri() { return ENDPOINT; }
            @Override public HttpClient.Version version() { return HttpClient.Version.HTTP_1_1; }
        };
    }

    /** A delegate that answers from a script, counting how often it was called. */
    private static final class ScriptedClient extends HttpClient {

        private final Deque<HttpResponse<byte[]>> scripted = new ArrayDeque<>();
        private final HttpResponse<byte[]> repeated;
        private final AtomicInteger sent = new AtomicInteger();

        ScriptedClient(List<HttpResponse<byte[]>> responses) {
            this.scripted.addAll(responses);
            this.repeated = null;
        }

        private ScriptedClient(HttpResponse<byte[]> repeated) {
            this.repeated = repeated;
        }

        static ScriptedClient always(HttpResponse<byte[]> response) {
            return new ScriptedClient(response);
        }

        @SuppressWarnings("unchecked")
        private <T> HttpResponse<T> next() {
            sent.incrementAndGet();
            HttpResponse<byte[]> answer = repeated != null ? repeated : scripted.poll();
            if (answer == null) {
                throw new IllegalStateException("the client sent more requests than the script has answers");
            }
            return (HttpResponse<T>) answer;
        }

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            return next();
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            return CompletableFuture.completedFuture(next());
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> handler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            return CompletableFuture.completedFuture(next());
        }

        @Override public Optional<CookieHandler> cookieHandler() { return Optional.empty(); }
        @Override public Optional<Duration> connectTimeout() { return Optional.empty(); }
        @Override public Redirect followRedirects() { return Redirect.NEVER; }
        @Override public Optional<ProxySelector> proxy() { return Optional.empty(); }
        @Override public SSLContext sslContext() { throw new UnsupportedOperationException(); }
        @Override public SSLParameters sslParameters() { throw new UnsupportedOperationException(); }
        @Override public Optional<Authenticator> authenticator() { return Optional.empty(); }
        @Override public Version version() { return Version.HTTP_1_1; }
        @Override public Optional<Executor> executor() { return Optional.empty(); }
    }
}
