// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.Set;

/**
 * When to re-send a throttled or transiently-failed VGI HTTP request, and how
 * long to wait first.
 *
 * <h2>Why a client needs this at all</h2>
 *
 * <p>{@code max_workers} on a plan is a <em>normative</em> cap on how many
 * splits a client may have in flight at once, not advice. A worker that is
 * already at its cap refuses the next redemption with {@code 429} and a
 * {@code Retry-After} telling the client when capacity is expected. A client
 * with no 429 handling reads that refusal as a failed request: at best the
 * whole scan dies on a condition the protocol calls routine, at worst the
 * response body — a proxy's HTML, a JSON envelope, nothing at all — is handed
 * to the Arrow decoder and the operator gets {@code empty IPC stream (no
 * schema)} for what was actually backpressure.</p>
 *
 * <h2>Jitter: full jitter, and why it is not optional</h2>
 *
 * <p>The delay carries a random component drawn per attempt, because the
 * failure this policy handles is <em>correlated across clients by
 * construction</em>. N readers of one plan hit the cap at the same instant and
 * are all told to come back in the same number of seconds; if each sleeps
 * exactly that, they wake together and re-collide, and the cap converts a
 * throttle into a synchronized herd that never drains — the precise failure
 * 429 exists to prevent.</p>
 *
 * <p>The strategy is <strong>full jitter</strong> (a uniform draw over the
 * whole backoff window, rather than {@code window/2 + rand(window/2)}
 * "equal jitter" or a fixed multiplier): it is the variant that minimises both
 * contention and total completion time in AWS's measurements, because it is the
 * only one whose <em>lower</em> bound also spreads — two clients that failed in
 * the same millisecond almost surely draw different sleeps, instead of merely
 * differing in the back half of a shared floor.</p>
 *
 * <p>Where the server supplied {@code Retry-After}, the jitter is
 * <strong>added to</strong> it rather than drawn within it. The header is an
 * instruction about when capacity returns; sleeping less than it would ignore
 * the server and arrive early to the same refusal, so the draw may only ever
 * push a client later, never earlier.</p>
 *
 * <h2>What is retryable</h2>
 *
 * <p>{@code 429} plus the gateway-transient class {@code 502}/{@code 503}/
 * {@code 504}: all four mean the request was <em>refused or never reached the
 * worker</em>, so re-sending it neither duplicates work nor re-runs a failure.
 * {@code 500} is deliberately excluded — a VGI HTTP server answers a failed
 * turn with {@code 500} and an Arrow error batch carrying the worker's own
 * exception, so that status means "your call ran and threw". Retrying it hides
 * a deterministic error behind N identical re-runs of it.</p>
 *
 * <p>Re-sending is sound for the scan path specifically because split
 * redemption is replayable by contract: the protocol states a split may be
 * redeemed more than once and that a re-redemption is not an error.</p>
 *
 * @see RetryingHttpClient
 */
public final class RetryPolicy {

    /**
     * The statuses this policy re-sends. See the class documentation for why
     * {@code 500} is not among them.
     */
    public static final Set<Integer> RETRYABLE_STATUSES = Set.of(429, 502, 503, 504);

    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final Duration DEFAULT_TOTAL_BUDGET = Duration.ofSeconds(120);
    private static final Duration DEFAULT_BASE_DELAY = Duration.ofMillis(250);
    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(20);

    private final int maxAttempts;
    private final Duration totalBudget;
    private final Duration baseDelay;
    private final Duration maxDelay;

    private RetryPolicy(int maxAttempts, Duration totalBudget, Duration baseDelay, Duration maxDelay) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1, got " + maxAttempts);
        }
        if (totalBudget.isNegative() || baseDelay.isNegative() || maxDelay.isNegative()) {
            throw new IllegalArgumentException("durations must not be negative");
        }
        this.maxAttempts = maxAttempts;
        this.totalBudget = totalBudget;
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
    }

    /**
     * The default policy: at most 5 attempts within 120 seconds, backing off
     * from 250&nbsp;ms to a 20-second window.
     *
     * <p>Both bounds are present on purpose. An attempt cap alone still lets a
     * client sit for the sum of five long {@code Retry-After}s; a time budget
     * alone still lets it hammer a server that answers instantly. The pair is
     * what makes "this scan will fail" reachable instead of "this scan hangs".</p>
     *
     * @return the default policy
     */
    public static RetryPolicy defaults() {
        return new RetryPolicy(DEFAULT_MAX_ATTEMPTS, DEFAULT_TOTAL_BUDGET,
                DEFAULT_BASE_DELAY, DEFAULT_MAX_DELAY);
    }

    /**
     * A policy with the given bounds.
     *
     * @param maxAttempts total attempts including the first, at least 1; 1 disables retrying
     * @param totalBudget wall-clock ceiling across all attempts of one request
     * @param baseDelay first backoff window, doubled per attempt
     * @param maxDelay ceiling for the backoff window
     * @return the policy
     */
    public static RetryPolicy of(int maxAttempts, Duration totalBudget, Duration baseDelay, Duration maxDelay) {
        return new RetryPolicy(maxAttempts, totalBudget, baseDelay, maxDelay);
    }

    /**
     * Whether this status is one the client may re-send.
     *
     * @param status the HTTP status code
     * @return true when the status is in {@link #RETRYABLE_STATUSES}
     */
    public static boolean isRetryable(int status) {
        return RETRYABLE_STATUSES.contains(status);
    }

    /**
     * The attempt bound.
     *
     * @return the maximum number of attempts, including the first
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * The time bound.
     *
     * @return the wall-clock ceiling across all attempts of one request
     */
    public Duration totalBudget() {
        return totalBudget;
    }

    /**
     * Decide what to do about one response.
     *
     * <p>Pure: it reads no clock, sleeps nothing, and draws no randomness — the
     * caller supplies {@code now} and {@code jitter}, so a table test can pin
     * every outcome.</p>
     *
     * @param status the HTTP status just received
     * @param retryAfter the raw {@code Retry-After} header value, or {@code null}
     *        when the response carried none; both RFC 9110 forms are accepted
     *        (delay-seconds and HTTP-date), and an unparseable value is treated
     *        as absent rather than fatal — a malformed hint from a proxy must
     *        not turn a retryable throttle into a hard failure
     * @param attempt 1-based number of the attempt that produced {@code status}
     * @param elapsed time already spent on this request across all attempts
     * @param now the instant to resolve an HTTP-date {@code Retry-After} against
     * @param jitter a draw in {@code [0, 1)}; values outside are clamped
     * @return whether to retry, and after how long
     */
    public RetryDecision decide(int status, String retryAfter, int attempt,
                                Duration elapsed, Instant now, double jitter) {
        if (!isRetryable(status)) {
            return RetryDecision.stop("HTTP " + status + " is not retryable");
        }
        if (attempt >= maxAttempts) {
            return RetryDecision.stop("attempt cap reached (" + maxAttempts + " attempts)");
        }

        long windowMillis = backoffWindowMillis(attempt);
        long jitterMillis = (long) (clamp(jitter) * windowMillis);
        Optional<Duration> advertised = parseRetryAfter(retryAfter, now);
        // Additive over Retry-After, uniform within the window without it — see
        // the jitter discussion in the class documentation.
        Duration delay = advertised
                .map(d -> d.plusMillis(jitterMillis))
                .orElseGet(() -> Duration.ofMillis(jitterMillis));

        if (elapsed.plus(delay).compareTo(totalBudget) > 0) {
            // Deliberately not clamped down to the remaining budget: a server
            // that says "60s" and a client that comes back in 3 is a client
            // that will be refused again. Better to fail now, with the reason.
            return RetryDecision.stop("waiting " + delay.toMillis() + "ms would exceed the "
                    + totalBudget.toMillis() + "ms total-time budget (already spent "
                    + elapsed.toMillis() + "ms)");
        }
        return RetryDecision.retryIn(delay, advertised.isPresent()
                ? "HTTP " + status + " with Retry-After " + advertised.get().toMillis() + "ms"
                : "HTTP " + status + " with no Retry-After");
    }

    /**
     * Parse a {@code Retry-After} header in either RFC 9110 form.
     *
     * @param header the raw header value, or {@code null}
     * @param now the instant an HTTP-date is measured from
     * @return the delay, floored at zero (a date already in the past means "now"),
     *         or empty when the header is absent or unparseable
     */
    public static Optional<Duration> parseRetryAfter(String header, Instant now) {
        if (header == null) {
            return Optional.empty();
        }
        String value = header.trim();
        if (value.isEmpty()) {
            return Optional.empty();
        }
        try {
            // delay-seconds. Fractions are not legal here, so a non-integer
            // falls through to the date parse rather than being truncated.
            long seconds = Long.parseLong(value);
            return Optional.of(Duration.ofSeconds(Math.max(0, seconds)));
        } catch (NumberFormatException notASecondsCount) {
            // fall through to the HTTP-date form
        }
        try {
            ZonedDateTime when = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME);
            Duration until = Duration.between(now, when.toInstant());
            return Optional.of(until.isNegative() ? Duration.ZERO : until);
        } catch (DateTimeParseException notADate) {
            return Optional.empty();
        }
    }

    /** Exponential window for this attempt, capped: base, 2×base, 4×base, … up to maxDelay. */
    private long backoffWindowMillis(int attempt) {
        int doublings = Math.min(Math.max(attempt, 1) - 1, 32);
        long window = baseDelay.toMillis() << doublings;
        return Math.min(window, maxDelay.toMillis());
    }

    private static double clamp(double jitter) {
        if (Double.isNaN(jitter) || jitter < 0.0) {
            return 0.0;
        }
        return Math.min(jitter, 1.0);
    }
}
