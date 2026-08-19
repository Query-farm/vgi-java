// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An {@link HttpClient} that honours VGI backpressure: it re-sends a request a
 * bounded number of times when the worker refuses it with {@code 429} (or a
 * transient gateway status), waiting as instructed by {@code Retry-After} plus
 * jitter, and fails legibly when it runs out of attempts or time.
 *
 * <p>It is a decorator rather than a new transport so it drops into the client
 * a JVM caller already has:</p>
 *
 * <pre>{@code
 * HttpRpcConnection conn = HttpRpcConnection.builder(endpoint)
 *         .httpClient(RetryingHttpClient.wrap(HttpClient.newHttpClient(), RetryPolicy.defaults()))
 *         .build();
 * VgiService worker = conn.proxy(VgiService.class);
 * }</pre>
 *
 * <p>Retrying happens below the RPC layer on purpose: the status code and
 * {@code Retry-After} are transport facts that never reach a decoded VGI
 * response, so a caller working with {@code VgiService} has no way to
 * distinguish "the worker is at its {@code max_workers} cap" from any other
 * failed call. Here they are still visible.</p>
 *
 * <p><strong>Scope.</strong> Only response <em>statuses</em> are retried, never
 * an {@link IOException} from the underlying client: a connection that dies
 * mid-exchange may have delivered the request, and re-sending a redemption
 * whose response was merely lost is a different (and less clearly safe) trade
 * than re-sending one the server explicitly refused.</p>
 *
 * @see RetryPolicy
 */
public final class RetryingHttpClient extends HttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingHttpClient.class);

    /** Arrow IPC streams open with a continuation marker; see {@link #requireDecodableErrorBody}. */
    private static final byte ARROW_CONTINUATION_BYTE = (byte) 0xFF;

    private final HttpClient delegate;
    private final RetryPolicy policy;
    private final Supplier<Instant> clock;
    private final Supplier<Double> jitterSource;

    private RetryingHttpClient(HttpClient delegate, RetryPolicy policy,
                               Supplier<Instant> clock, Supplier<Double> jitterSource) {
        this.delegate = delegate;
        this.policy = policy;
        this.clock = clock;
        this.jitterSource = jitterSource;
    }

    /**
     * Wrap a client with the given policy.
     *
     * @param delegate the client that actually sends; not closed by this wrapper's
     *        constructor and closed by {@link #close()} exactly as if the caller
     *        had closed it directly
     * @param policy when to retry and how long to wait
     * @return the wrapping client
     */
    public static RetryingHttpClient wrap(HttpClient delegate, RetryPolicy policy) {
        return new RetryingHttpClient(delegate, policy, Instant::now,
                () -> ThreadLocalRandom.current().nextDouble());
    }

    /**
     * Wrap a client with an injected clock and jitter source.
     *
     * <p>For tests: a real jitter draw makes the sleep — and therefore the test —
     * nondeterministic, and a real clock makes the time budget wall-clock
     * dependent.</p>
     *
     * @param delegate the client that actually sends
     * @param policy when to retry and how long to wait
     * @param clock supplies "now", used to resolve an HTTP-date {@code Retry-After}
     *        and to measure the elapsed budget
     * @param jitterSource supplies the per-attempt draw in {@code [0, 1)}
     * @return the wrapping client
     */
    public static RetryingHttpClient wrap(HttpClient delegate, RetryPolicy policy,
                                          Supplier<Instant> clock, Supplier<Double> jitterSource) {
        return new RetryingHttpClient(delegate, policy, clock, jitterSource);
    }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
            throws IOException, InterruptedException {
        Instant started = clock.get();
        for (int attempt = 1; ; attempt++) {
            HttpResponse<T> response = delegate.send(request, responseBodyHandler);
            if (!RetryPolicy.isRetryable(response.statusCode())) {
                return requireDecodableErrorBody(response);
            }
            String retryAfter = header(response, "retry-after");
            Instant now = clock.get();
            RetryDecision decision = policy.decide(response.statusCode(), retryAfter, attempt,
                    Duration.between(started, now), now, jitterSource.get());
            if (!decision.retry()) {
                throw new RetryBudgetExhaustedException(request.uri(), attempt,
                        response.statusCode(), retryAfter, decision.reason());
            }
            discard(response);
            LOG.debug("vgi: retrying {} in {}ms (attempt {} of {}): {}",
                    request.uri(), decision.delay().toMillis(), attempt + 1,
                    policy.maxAttempts(), decision.reason());
            Thread.sleep(decision.delay().toMillis());
        }
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(
            HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
        return sendAsync(request, responseBodyHandler, null);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(
            HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler,
            HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
        // The same policy on the async path. Leaving it un-retried would be the
        // worse trap of the two: a caller who wrapped their client would get
        // backpressure handling on send() and none here, with nothing in the
        // types to say so.
        return attemptAsync(request, responseBodyHandler, pushPromiseHandler, clock.get(), 1);
    }

    private <T> CompletableFuture<HttpResponse<T>> attemptAsync(
            HttpRequest request, HttpResponse.BodyHandler<T> bodyHandler,
            HttpResponse.PushPromiseHandler<T> pushPromiseHandler, Instant started, int attempt) {
        CompletableFuture<HttpResponse<T>> sent = pushPromiseHandler == null
                ? delegate.sendAsync(request, bodyHandler)
                : delegate.sendAsync(request, bodyHandler, pushPromiseHandler);
        return sent.thenCompose(response -> {
            if (!RetryPolicy.isRetryable(response.statusCode())) {
                try {
                    return CompletableFuture.completedFuture(requireDecodableErrorBody(response));
                } catch (IOException e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
            String retryAfter = header(response, "retry-after");
            Instant now = clock.get();
            RetryDecision decision = policy.decide(response.statusCode(), retryAfter, attempt,
                    Duration.between(started, now), now, jitterSource.get());
            if (!decision.retry()) {
                return CompletableFuture.failedFuture(new RetryBudgetExhaustedException(
                        request.uri(), attempt, response.statusCode(), retryAfter, decision.reason()));
            }
            discard(response);
            Executor later = CompletableFuture.delayedExecutor(
                    decision.delay().toMillis(), TimeUnit.MILLISECONDS,
                    delegate.executor().orElse(Runnable::run));
            return CompletableFuture
                    .supplyAsync(() -> attemptAsync(request, bodyHandler, pushPromiseHandler,
                            started, attempt + 1), later)
                    .thenCompose(next -> next);
        });
    }

    /**
     * Fail loudly on an error response that claims to carry an Arrow IPC stream
     * and does not.
     *
     * <p>A {@code 4xx}/{@code 5xx} from something that is not the worker — a
     * proxy, a load balancer, an auth gateway — can arrive stamped with the
     * Arrow content type and an empty or HTML body. The RPC layer trusts the
     * content type and hands those bytes to the IPC reader, which reports
     * {@code empty IPC stream (no schema)}: a message about Arrow framing for a
     * problem that was an HTTP status, and one that has already cost real
     * debugging time in a sibling SDK. Catching it here, where the status is
     * still in hand, keeps the error about the thing that actually went
     * wrong.</p>
     *
     * <p>Only responses that <em>claim</em> Arrow are inspected: everything else
     * is passed through untouched so the RPC layer keeps its own handling — in
     * particular a {@code 401}, which it must classify as an auth failure rather
     * than a transport one.</p>
     */
    private static <T> HttpResponse<T> requireDecodableErrorBody(HttpResponse<T> response) throws IOException {
        if (response.statusCode() < 400
                || !(response.body() instanceof byte[] body)
                || !claimsArrow(response)) {
            return response;
        }
        // Content-Encoding means the bytes are still compressed, so the framing
        // check below would be reading the codec's header, not Arrow's.
        if (header(response, "content-encoding") != null) {
            return response;
        }
        if (body.length >= 8 && body[0] == ARROW_CONTINUATION_BYTE && body[1] == ARROW_CONTINUATION_BYTE
                && body[2] == ARROW_CONTINUATION_BYTE && body[3] == ARROW_CONTINUATION_BYTE) {
            // A real Arrow error stream: the worker's own exception rides inside
            // it, and the RPC layer surfaces that with the worker's message.
            return response;
        }
        throw new IOException("HTTP " + response.statusCode() + " from " + response.uri()
                + " declared an Arrow IPC body but sent " + body.length
                + " bytes that are not an Arrow stream"
                + preview(body));
    }

    private static boolean claimsArrow(HttpResponse<?> response) {
        String contentType = header(response, "content-type");
        return contentType != null && contentType.toLowerCase(java.util.Locale.ROOT).contains("arrow");
    }

    private static String preview(byte[] body) {
        if (body.length == 0) {
            return "";
        }
        String text = new String(body, 0, Math.min(body.length, 200), StandardCharsets.UTF_8)
                .replace('\n', ' ').trim();
        return text.isEmpty() ? "" : " — " + text;
    }

    private static String header(HttpResponse<?> response, String name) {
        return response.headers().firstValue(name).orElse(null);
    }

    /** Release a refused response's body before re-sending, for handlers that stream it. */
    private static void discard(HttpResponse<?> response) {
        if (response.body() instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // A body we are throwing away failing to close is not worth
                // failing the retry over.
            }
        }
    }

    // ------------------------------------------------------------------
    // Everything below is plain delegation: the wrapper changes when a
    // request is repeated, never how it is configured or sent.
    // ------------------------------------------------------------------

    @Override
    public Optional<CookieHandler> cookieHandler() {
        return delegate.cookieHandler();
    }

    @Override
    public Optional<Duration> connectTimeout() {
        return delegate.connectTimeout();
    }

    @Override
    public Redirect followRedirects() {
        return delegate.followRedirects();
    }

    @Override
    public Optional<ProxySelector> proxy() {
        return delegate.proxy();
    }

    @Override
    public SSLContext sslContext() {
        return delegate.sslContext();
    }

    @Override
    public SSLParameters sslParameters() {
        return delegate.sslParameters();
    }

    @Override
    public Optional<Authenticator> authenticator() {
        return delegate.authenticator();
    }

    @Override
    public Version version() {
        return delegate.version();
    }

    @Override
    public Optional<Executor> executor() {
        return delegate.executor();
    }

    @Override
    public WebSocket.Builder newWebSocketBuilder() {
        return delegate.newWebSocketBuilder();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public void shutdownNow() {
        delegate.shutdownNow();
    }

    @Override
    public boolean awaitTermination(Duration duration) throws InterruptedException {
        return delegate.awaitTermination(duration);
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }
}
