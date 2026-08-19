// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import java.io.IOException;
import java.net.URI;

/**
 * Thrown when a VGI HTTP request kept being refused and the client ran out of
 * the attempts or the time it was allowed to spend on it.
 *
 * <p>This exists so the give-up is <em>legible</em>. A client that silently
 * keeps retrying a capped worker looks like a hung query, and one that surfaces
 * the raw last response looks like a protocol error; either way the operator
 * ends up reading worker logs to discover that the answer was backpressure. The
 * message names the endpoint, how many attempts were made, and the last status
 * and {@code Retry-After} seen, which is enough to tell "the worker is at its
 * {@code max_workers} cap and my scan is too wide" from "this endpoint is
 * broken" without leaving the stack trace.</p>
 *
 * <p>It extends {@link IOException} so it travels the transport error path of
 * whatever RPC layer is driving: {@code HttpRpcConnection} turns an
 * {@code IOException} from the client into an {@code RpcError} of type
 * {@code TransportError}, carrying this message along.</p>
 */
public class RetryBudgetExhaustedException extends IOException {

    private static final long serialVersionUID = 1L;

    /** The request URI that kept failing. @serial */
    private final URI endpoint;
    /** How many attempts were made in total. @serial */
    private final int attempts;
    /** The HTTP status of the final attempt. @serial */
    private final int lastStatus;

    /**
     * Build the give-up error.
     *
     * @param endpoint the request URI that kept failing
     * @param attempts how many attempts were made in total
     * @param lastStatus the HTTP status of the final attempt
     * @param retryAfter the final response's {@code Retry-After} value, or {@code null}
     * @param reason why the client stopped, from {@link RetryDecision#reason()}
     */
    public RetryBudgetExhaustedException(URI endpoint, int attempts, int lastStatus,
                                         String retryAfter, String reason) {
        super("gave up on " + endpoint + " after " + attempts + " attempt"
                + (attempts == 1 ? "" : "s") + ": last response HTTP " + lastStatus
                + (retryAfter == null ? " with no Retry-After" : " with Retry-After: " + retryAfter)
                + " — " + reason);
        this.endpoint = endpoint;
        this.attempts = attempts;
        this.lastStatus = lastStatus;
    }

    /**
     * The request URI that kept failing.
     *
     * @return the endpoint
     */
    public URI endpoint() {
        return endpoint;
    }

    /**
     * How many attempts were made in total, the first included.
     *
     * @return the attempt count
     */
    public int attempts() {
        return attempts;
    }

    /**
     * The status of the final attempt — {@code 429} when the worker was at its
     * cap throughout, a gateway status when it never answered.
     *
     * @return the last HTTP status seen
     */
    public int lastStatus() {
        return lastStatus;
    }
}
