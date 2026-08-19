// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import java.time.Duration;

/**
 * What a client should do with one throttled or failed HTTP response.
 *
 * <p>The decision is separated from the act of sleeping so it can be decided in
 * a pure function ({@link RetryPolicy#decide}) and asserted in a table, without
 * a server, a clock, or a real delay. Every input that moves the outcome —
 * status, {@code Retry-After}, attempt number, elapsed time, jitter draw — is a
 * parameter, so a disagreement about backoff is a unit-test row rather than an
 * afternoon watching a worker under load.</p>
 *
 * @param retry whether the caller should send the request again
 * @param delay how long to wait first; {@link Duration#ZERO} when {@code retry}
 *        is false, and possibly zero when it is true (a full-jitter draw may
 *        land at the bottom of its window)
 * @param reason a human-readable explanation, carried into the give-up error so
 *        an operator learns <em>which</em> bound stopped the client — the
 *        attempt cap, the time budget, or a status nobody should retry
 */
public record RetryDecision(boolean retry, Duration delay, String reason) {

    /**
     * A decision to retry after waiting.
     *
     * @param delay how long to wait before the next attempt
     * @param reason why this response is being retried
     * @return the decision
     */
    public static RetryDecision retryIn(Duration delay, String reason) {
        return new RetryDecision(true, delay, reason);
    }

    /**
     * A decision to give up and surface the failure.
     *
     * @param reason why no further attempt will be made
     * @return the decision
     */
    public static RetryDecision stop(String reason) {
        return new RetryDecision(false, Duration.ZERO, reason);
    }
}
