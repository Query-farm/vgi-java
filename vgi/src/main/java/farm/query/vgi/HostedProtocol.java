// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import java.util.Objects;

/**
 * One additional vgi-rpc application protocol a worker hosts beside {@code vgi.v2}: the
 * protocol's service interface and the object its calls are dispatched to.
 *
 * <p>Returned by the hook a worker installs with {@link Worker#hostedProtocols}. The interface
 * names the protocol with {@code @ProtocolName} (and optionally versions it with
 * {@code @ProtocolVersion}); its routing key must be distinct from {@code vgi.v2} and from every
 * other hosted protocol, and may not use the reserved {@code vgi_rpc.} prefix.
 *
 * @param protocol the protocol's service interface
 * @param implementation the object calls are dispatched to; must implement {@code protocol}
 */
public record HostedProtocol(Class<?> protocol, Object implementation) {

    /** Refuse nulls up front, so the hook's mistake is reported as the hook's. */
    public HostedProtocol {
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(implementation, "implementation");
    }

    /**
     * Pair a protocol with its implementation, checked at compile time.
     *
     * @param protocol the protocol's service interface
     * @param implementation the object its calls are dispatched to
     * @param <T> the protocol type
     * @return the pair
     */
    public static <T> HostedProtocol of(Class<T> protocol, T implementation) {
        return new HostedProtocol(protocol, implementation);
    }
}
