// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.http.IrohPeerIdentityProviders;
import farm.query.vgirpc.identity.PeerAuthenticationPolicies;
import farm.query.vgirpc.transport.TcpServerOptions;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Trust boundary between a loopback VGI worker and {@code vgi-iroh-bridge}. */
public record IrohBridgeOptions(
        String issuer,
        Set<String> trustedProxyAddresses,
        boolean authenticate) {

    /** Authenticate bridge-verified EndpointIds from a loopback bridge. */
    public IrohBridgeOptions(String issuer) {
        this(issuer, Set.of("127.0.0.1"), true);
    }

    public IrohBridgeOptions {
        if (issuer == null || issuer.isBlank()) {
            throw new IllegalArgumentException("Iroh bridge issuer is required");
        }
        trustedProxyAddresses = trustedProxyAddresses == null || trustedProxyAddresses.isEmpty()
                ? Set.of("127.0.0.1")
                : Set.copyOf(trustedProxyAddresses);
    }

    /** Apply identity forwarding to an HTTP worker configuration. */
    public HttpServer.Config.Builder apply(HttpServer.Config.Builder builder) {
        var provider = IrohPeerIdentityProviders.forwarded(issuer, trustedProxyAddresses);
        return builder
                .peerIdentityProviders(List.of(provider))
                .peerAuthenticationPolicy(authenticate
                        ? PeerAuthenticationPolicies.primary("iroh")
                        : PeerAuthenticationPolicies::observe);
    }

    /** Build the strict PROXY-v2 configuration used by the raw bridge upstream. */
    TcpServerOptions tcpServerOptions() {
        return TcpServerOptions.builder()
                .proxyProtocolV2Required(true)
                .trustedProxyAddresses(trustedProxyAddresses)
                .irohProxyIssuer(issuer)
                .peerAuthenticationPolicy(authenticate
                        ? PeerAuthenticationPolicies.primary("iroh")
                        : PeerAuthenticationPolicies::observe)
                .build();
    }

    static IrohBridgeOptions fromArgs(
            String issuer, List<String> trustedProxyAddresses, boolean authenticate) {
        return new IrohBridgeOptions(
                issuer,
                trustedProxyAddresses.isEmpty()
                        ? Set.of("127.0.0.1")
                        : new LinkedHashSet<>(trustedProxyAddresses),
                authenticate);
    }
}
