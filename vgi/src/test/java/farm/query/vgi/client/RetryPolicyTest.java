// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The retry decision as a truth table.
 *
 * <p>{@link RetryPolicy#decide} takes its clock and its jitter draw as
 * arguments precisely so this can be a table instead of a mock server plus a
 * stopwatch: every row here would otherwise be a timing-sensitive integration
 * test, and the ones that matter most — the give-up bounds — would be the
 * slowest of them.</p>
 */
class RetryPolicyTest {

    private static final Instant NOW = Instant.parse("2026-08-19T12:00:00Z");

    /** One row: a response, and what the client should do about it. */
    private record Case(String label, int status, String retryAfter, int attempt,
                        Duration elapsed, double jitter, boolean retry, long delayMillis) {
        @Override
        public String toString() {
            return label;
        }
    }

    private static String httpDate(Instant when) {
        return DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.ofInstant(when, ZoneOffset.UTC));
    }

    private static List<Case> decisions() {
        return List.of(
                // Retry-After is obeyed to the millisecond at the bottom of the
                // jitter draw, and only ever extended by it — never shortened,
                // or the client arrives early to the same refusal.
                new Case("429 Retry-After 2s, no jitter", 429, "2", 1,
                        Duration.ZERO, 0.0, true, 2000),
                new Case("429 Retry-After 2s, full jitter draw", 429, "2", 1,
                        Duration.ZERO, 1.0, true, 2250),

                // Without a header the whole delay IS the jitter draw: full
                // jitter, uniform over the backoff window from zero up.
                new Case("429 bare, jitter 0", 429, null, 1, Duration.ZERO, 0.0, true, 0),
                new Case("429 bare, jitter 1", 429, null, 1, Duration.ZERO, 1.0, true, 250),
                new Case("429 bare, window doubles by attempt", 429, null, 3,
                        Duration.ZERO, 1.0, true, 1000),
                new Case("429 bare, last allowed attempt", 429, null, 4,
                        Duration.ZERO, 1.0, true, 2000),
                new Case("jitter draw above 1 is clamped", 429, null, 1,
                        Duration.ZERO, 7.5, true, 250),
                new Case("negative jitter draw is clamped", 429, null, 1,
                        Duration.ZERO, -1.0, true, 0),

                // The whole retryable class, not just 429.
                new Case("502 is retryable", 502, "1", 1, Duration.ZERO, 0.0, true, 1000),
                new Case("503 is retryable", 503, "1", 1, Duration.ZERO, 0.0, true, 1000),
                new Case("504 is retryable", 504, "1", 1, Duration.ZERO, 0.0, true, 1000),

                // A 500 carries the worker's own exception in an Arrow error
                // batch: re-sending it just re-runs a deterministic failure.
                new Case("500 is not retried", 500, "1", 1, Duration.ZERO, 0.0, false, 0),
                new Case("404 is not retried", 404, null, 1, Duration.ZERO, 0.0, false, 0),
                new Case("401 is not retried", 401, null, 1, Duration.ZERO, 0.0, false, 0),

                // Retry-After: HTTP-date form.
                new Case("HTTP-date 30s out", 503, httpDate(NOW.plusSeconds(30)), 1,
                        Duration.ZERO, 0.0, true, 30_000),
                new Case("HTTP-date already past floors at zero", 503,
                        httpDate(NOW.minusSeconds(30)), 1, Duration.ZERO, 0.0, true, 0),

                // A malformed hint must not turn a retryable throttle into a
                // hard failure; it degrades to plain backoff.
                new Case("unparseable Retry-After degrades to backoff", 429, "soon", 1,
                        Duration.ZERO, 1.0, true, 250),
                new Case("empty Retry-After degrades to backoff", 429, "  ", 1,
                        Duration.ZERO, 1.0, true, 250),
                new Case("padded delay-seconds parse", 429, "  3  ", 1,
                        Duration.ZERO, 0.0, true, 3000),

                // Both bounds stop the client, and say which one did.
                new Case("attempt cap", 429, "1", 5, Duration.ZERO, 0.0, false, 0),
                new Case("time budget", 429, "600", 1, Duration.ZERO, 0.0, false, 0),
                new Case("time already spent counts against the budget", 429, "30",
                        2, Duration.ofSeconds(100), 0.0, false, 0));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("decisions")
    void decidesFromStatusHeaderAndBounds(Case c) {
        RetryDecision decision = RetryPolicy.defaults()
                .decide(c.status(), c.retryAfter(), c.attempt(), c.elapsed(), NOW, c.jitter());

        assertEquals(c.retry(), decision.retry(), () -> c.label() + ": " + decision.reason());
        assertEquals(c.delayMillis(), decision.delay().toMillis(), () -> c.label() + " delay");
    }

    @Test
    void giveUpReasonNamesTheBoundThatStopped() {
        RetryPolicy policy = RetryPolicy.defaults();

        assertTrue(policy.decide(429, "1", 5, Duration.ZERO, NOW, 0.0).reason().contains("attempt cap"),
                "an operator has to be able to tell the attempt cap from the time budget");
        assertTrue(policy.decide(429, "600", 1, Duration.ZERO, NOW, 0.0).reason().contains("budget"));
        assertTrue(policy.decide(500, null, 1, Duration.ZERO, NOW, 0.0).reason().contains("500"));
    }

    @Test
    void backoffWindowStopsDoubling() {
        // Left un-capped the window doubles into hours, which turns a slow
        // recovery into a client that looks hung rather than one that gives up.
        RetryPolicy patient = RetryPolicy.of(20, Duration.ofHours(1),
                Duration.ofMillis(250), Duration.ofSeconds(20));
        assertEquals(20_000,
                patient.decide(429, null, 12, Duration.ZERO, NOW, 1.0).delay().toMillis());
    }

    @Test
    void singleAttemptPolicyNeverRetries() {
        RetryPolicy noRetries = RetryPolicy.of(1, Duration.ofSeconds(60),
                Duration.ofMillis(250), Duration.ofSeconds(20));
        assertFalse(noRetries.decide(429, "1", 1, Duration.ZERO, NOW, 0.0).retry());
    }

    @Test
    void retryAfterParsesBothFormsAndRejectsNeither() {
        assertEquals(Duration.ofSeconds(7), RetryPolicy.parseRetryAfter("7", NOW).orElseThrow());
        assertEquals(Duration.ofSeconds(45),
                RetryPolicy.parseRetryAfter(httpDate(NOW.plusSeconds(45)), NOW).orElseThrow());
        assertTrue(RetryPolicy.parseRetryAfter(null, NOW).isEmpty());
        assertTrue(RetryPolicy.parseRetryAfter("later", NOW).isEmpty());
        // Not "0.5 seconds" and not truncated to 0: fractions are illegal in
        // delay-seconds, so the value is treated as a (failed) date instead.
        assertTrue(RetryPolicy.parseRetryAfter("0.5", NOW).isEmpty());
        // A server that clamps to a negative delay still gets a sane answer.
        assertEquals(Duration.ZERO, RetryPolicy.parseRetryAfter("-5", NOW).orElseThrow());
    }
}
