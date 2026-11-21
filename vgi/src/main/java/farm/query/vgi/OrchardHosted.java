// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.http.auth.JwtAuthenticator;
import jakarta.servlet.http.HttpServletRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Orchard hosted mode ({@code VGI_ORCHARD_HOSTED=1}).
 *
 * <p>A hosted worker (a <em>Field Worker</em>) serves VGI over HTTP to callers who hold a
 * <em>Pass</em>: a short-lived RFC 9068 access token ({@code typ: at+jwt}, ES256) issued by the
 * Orchard Front Desk for exactly one canonical Desk Address. The worker verifies Passes locally
 * against the Front Desk's JWKS and accepts nothing else. Everything here fails closed:
 * <ul>
 *   <li>Startup is refused unless {@code VGI_JWT_ISSUER}, {@code VGI_JWT_AUDIENCE} and
 *       {@code VGI_JWT_JWKS_URI} are each set to a single non-empty value, and
 *       {@code VGI_SIGNING_KEY} (or {@link Worker#signingKey(byte[])}) supplies a stable key of
 *       at least 32 bytes.</li>
 *   <li>Startup is refused when any other credential is configured: static bearer tokens,
 *       sealed-grant keys, a {@link Worker#resolveToken} / {@link Worker#mintGrant} hook, OAuth
 *       resource metadata, a proxy-proof gate, peer identity (Iroh, Tailscale, SPIFFE ...), a custom
 *       {@link Authenticator}, or a transport other than HTTP.</li>
 *   <li>Every request except {@code HEAD /health} (and CORS preflights, which carry no
 *       credentials) must present a valid Pass, or gets a 401 with
 *       {@code WWW-Authenticate: Bearer error="invalid_token"}.</li>
 *   <li>The JWKS is cached for at most 10 minutes; once that passes and the re-fetch fails,
 *       verification fails (HTTP 503) rather than using stale keys.</li>
 * </ul>
 *
 * <p>Function code sees the Pass's claims through the usual auth path
 * ({@code CallContext.auth()}, or {@code AuthScope.current().auth()} from a scalar), with their
 * JSON types; the principal is {@code sub} and the domain is {@value #DOMAIN}.
 *
 * <p>This is the Java port of vgi-python's {@code vgi.hosted}; the design is in the extension
 * repository's {@code docs/location-delegation.md} ("The Pass" and "The Field Worker: SDK hosted
 * mode").
 */
public final class OrchardHosted {

    /** Environment variable that turns hosted mode on. Baked into the hosted base image. */
    public static final String HOSTED_ENV = "VGI_ORCHARD_HOSTED";
    /** The challenge on every hosted-mode 401. No {@code resource_metadata}: a Field Worker never
     *  sends a client off to discover an authorization server. */
    public static final String CHALLENGE = "Bearer error=\"invalid_token\"";
    /** {@link AuthContext#domain()} of a verified Pass. */
    public static final String DOMAIN = "orchard";
    /** Default and only-by-override signing algorithm for Passes. */
    public static final List<String> DEFAULT_ALGORITHMS = List.of("ES256");
    /** Required JOSE {@code typ} (RFC 9068). */
    public static final String REQUIRED_TYP = "at+jwt";
    /** Clock skew allowed on {@code exp} / {@code nbf}, in seconds. */
    public static final int LEEWAY_SECONDS = 60;
    /** Hard upper bound on how long JWKS keys are trusted, in seconds. */
    public static final long JWKS_MAX_AGE_SECONDS = 600;
    /** Minimum seconds between JWKS re-fetches triggered by an unknown {@code kid}. */
    public static final long JWKS_REFRESH_MIN_INTERVAL_SECONDS = 60;
    /** Timeout for each JWKS request. */
    public static final Duration JWKS_FETCH_TIMEOUT = Duration.ofSeconds(10);
    /** The shortest {@code VGI_SIGNING_KEY} a hosted worker accepts. */
    public static final int MIN_SIGNING_KEY_BYTES = 32;

    private static final Set<String> TRUE = Set.of("1", "true", "yes", "on");
    private static final Set<String> FALSE = Set.of("", "0", "false", "no", "off");

    private OrchardHosted() {}

    /**
     * A hosted-mode startup refusal. Its message is the complete operator-facing error line
     * ({@code Error: VGI_ORCHARD_HOSTED=1 (Orchard hosted mode): ...}); {@link Worker#runFromArgs}
     * prints it and exits with status 1.
     */
    public static final class RefusedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        RefusedException(String message) { super(message); }
    }

    /**
     * Verified hosted-mode settings, read from the environment.
     *
     * @param issuer     the Front Desk issuer ({@code iss}), matched exactly
     * @param audience   the one canonical Desk Address ({@code aud}), matched exactly
     * @param jwksUri    where the Front Desk publishes its signing keys
     * @param algorithms accepted JWS algorithms ({@code ES256} unless overridden)
     */
    public record Config(String issuer, String audience, String jwksUri, List<String> algorithms) {
        /** Copies {@code algorithms}. */
        public Config {
            algorithms = List.copyOf(algorithms);
        }
    }

    static RefusedException refusal(String reason) {
        return new RefusedException("Error: " + HOSTED_ENV + "=1 (Orchard hosted mode): " + reason);
    }

    /**
     * Whether {@code VGI_ORCHARD_HOSTED} turns hosted mode on.
     *
     * @return {@code true} for {@code 1}/{@code true}/{@code yes}/{@code on} (any case),
     *     {@code false} when unset or {@code 0}/{@code false}/{@code no}/{@code off}
     * @throws RefusedException for any other value: a typo must not quietly start a worker
     *     without authentication
     */
    public static boolean enabled() {
        return enabled(System.getenv());
    }

    static boolean enabled(Map<String, String> env) {
        String raw = env.get(HOSTED_ENV);
        if (raw == null) return false;
        String value = raw.strip().toLowerCase(Locale.ROOT);
        if (TRUE.contains(value)) return true;
        if (FALSE.contains(value)) return false;
        throw new RefusedException("Error: " + HOSTED_ENV + " must be 1 or 0 (or true/false), got '" + raw + "'");
    }

    private static String singleValue(Map<String, String> env, String name) {
        String value = env.getOrDefault(name, "").strip();
        if (value.isEmpty()) {
            throw refusal(name + " must be set (unset and empty are both errors).");
        }
        if (value.indexOf(',') >= 0) {
            throw refusal(name + " must be exactly one value; got a comma-separated list. "
                    + "A Pass is issued for one canonical Desk Address and one issuer.");
        }
        return value;
    }

    /**
     * Read and validate the hosted-mode environment.
     *
     * @return the configuration
     * @throws RefusedException when a required variable is missing, empty or multi-valued, or
     *     {@code VGI_JWT_ALGORITHMS} names {@code none} or an HMAC algorithm
     */
    public static Config resolveConfig() {
        return resolveConfig(System.getenv());
    }

    static Config resolveConfig(Map<String, String> env) {
        String issuer = singleValue(env, "VGI_JWT_ISSUER");
        String audience = singleValue(env, "VGI_JWT_AUDIENCE");
        String jwksUri = singleValue(env, "VGI_JWT_JWKS_URI");
        String rawAlgorithms = env.get("VGI_JWT_ALGORITHMS");
        List<String> algorithms;
        if (rawAlgorithms == null || rawAlgorithms.isBlank()) {
            algorithms = DEFAULT_ALGORITHMS;
        } else {
            algorithms = new ArrayList<>();
            for (String a : rawAlgorithms.split(",")) {
                if (!a.isBlank()) algorithms.add(a.strip());
            }
            boolean forbidden = algorithms.stream().map(a -> a.toLowerCase(Locale.ROOT))
                    .anyMatch(a -> a.equals("none") || a.startsWith("hs"));
            if (algorithms.isEmpty() || forbidden) {
                throw refusal("VGI_JWT_ALGORITHMS='" + rawAlgorithms + "' is not allowed; Passes are asymmetric "
                        + "(default ES256), never 'none' or HS*.");
            }
        }
        return new Config(issuer, audience, jwksUri, algorithms);
    }

    /**
     * Build the Pass verifier for {@code config}: ES256 (or the override), {@code typ: at+jwt},
     * exact {@code iss}, exactly one matching {@code aud}, required {@code exp}, 60 s leeway, and a
     * JWKS cache that fails closed. Claims keep their JSON types.
     *
     * @param config hosted-mode settings from {@link #resolveConfig()}
     * @return the verifier, recognised by {@link #isPassAuthenticator}
     * @throws RefusedException when the configuration cannot build a verifier (a malformed
     *     {@code VGI_JWT_JWKS_URI} or an unknown algorithm name)
     */
    public static Authenticator passAuthenticator(Config config) {
        try {
            return new PassAuthenticator(JwtAuthenticator.builder()
                    .issuer(config.issuer())
                    .audience(config.audience())
                    .jwksUri(config.jwksUri())
                    .allowedAlgorithmNames(config.algorithms())
                    .requiredTyp(REQUIRED_TYP)
                    .singleAudience(true)
                    .leewaySeconds(LEEWAY_SECONDS)
                    .jwksMaxAgeSeconds(JWKS_MAX_AGE_SECONDS)
                    .jwksRefreshMinIntervalSeconds(JWKS_REFRESH_MIN_INTERVAL_SECONDS)
                    .jwksFetchTimeout(JWKS_FETCH_TIMEOUT)
                    .domain(DOMAIN)
                    .jsonClaims(true)
                    .challenge(CHALLENGE)
                    .build());
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw refusal("cannot build the Pass verifier: " + e.getMessage());
        }
    }

    /**
     * Whether {@code authenticator} was built by {@link #passAuthenticator}.
     *
     * @param authenticator any authenticator, or {@code null}
     * @return {@code true} only for the hosted Pass verifier
     */
    public static boolean isPassAuthenticator(Authenticator authenticator) {
        return authenticator instanceof PassAuthenticator;
    }

    /** The hosted Pass verifier: a marker type around the hardened {@link JwtAuthenticator}. */
    private record PassAuthenticator(JwtAuthenticator delegate) implements Authenticator {
        @Override
        public AuthContext authenticate(HttpServletRequest request) throws AuthException {
            return delegate.authenticate(request);
        }
    }

    /**
     * Refuse to start when the environment configures another credential.
     *
     * @throws RefusedException on {@code VGI_BEARER_TOKENS}, {@code VGI_RPC_GRANT_KEYS},
     *     {@code VGI_OAUTH_RESOURCE} or an active {@code VGI_PROXY_PROOF_MODE}
     */
    static void checkEnvironmentConflicts(Map<String, String> env) {
        String[][] conflicts = {
                {"VGI_BEARER_TOKENS", "static bearer tokens"},
                {farm.query.vgirpc.identity.GrantKeys.KEYS_ENV, "sealed-grant keys"},
                {"VGI_OAUTH_RESOURCE",
                        "OAuth resource metadata (a Field Worker never advertises resource_metadata)"},
        };
        for (String[] c : conflicts) {
            if (!env.getOrDefault(c[0], "").isBlank()) {
                throw refusal(c[0] + " is set (" + c[1] + "); the only credential a hosted worker accepts is a Pass.");
            }
        }
        String proofMode = env.getOrDefault("VGI_PROXY_PROOF_MODE", "off").strip().toLowerCase(Locale.ROOT);
        if (!proofMode.isEmpty() && !proofMode.equals("off")) {
            throw refusal("VGI_PROXY_PROOF_MODE must be off; hosted workers authenticate by Pass only.");
        }
    }

    /**
     * Refuse a hosted worker whose signing key isn't stable across restarts.
     *
     * <p>The key seals catalog opaque data and HTTP state tokens. A Field Worker's machines stop
     * and start, and a deployment can run on several of them: a key minted per process makes every
     * restart (and every other machine) reject the sessions DuckDB already holds.
     *
     * @param signingKey the configured key material, or {@code null} when there is none
     * @param configured whether the operator configured it ({@code VGI_SIGNING_KEY} or
     *     {@link Worker#signingKey(byte[])}), rather than a parent process minting it
     * @throws RefusedException when the key is missing, minted, or shorter than 32 bytes
     */
    static void checkSigningKey(byte[] signingKey, boolean configured) {
        if (!configured || signingKey == null) {
            throw refusal("VGI_SIGNING_KEY is not set; hosted workers need a stable key shared by every machine "
                    + "of the deployment, or every restart breaks attached sessions.");
        }
        if (signingKey.length < MIN_SIGNING_KEY_BYTES) {
            throw refusal("VGI_SIGNING_KEY must be at least " + MIN_SIGNING_KEY_BYTES + " bytes.");
        }
    }

    /**
     * Refuse any HTTP server configuration that admits a non-Pass caller.
     *
     * @param config the configuration the worker was asked to serve
     * @throws RefusedException on the first conflict found
     */
    static void checkHttpConfig(HttpServer.Config config) {
        if (config.authenticator() != null && config.authenticator() != Authenticator.ANONYMOUS
                && !isPassAuthenticator(config.authenticator())) {
            throw refusal("a custom authenticator is configured; hosted workers verify Passes only.");
        }
        if (!config.peerIdentityProviders().isEmpty() || config.peerAuthenticationPolicy() != null) {
            throw refusal("peer identity (Iroh, Tailscale, SPIFFE, ...) is configured; "
                    + "hosted workers accept Passes only.");
        }
        if (config.proxyProofRequired() || !config.proxyAuthHeaders().isEmpty()) {
            throw refusal("a proxy proof or proxy-injected identity is required; "
                    + "hosted workers authenticate by Pass only.");
        }
    }

    /** slf4j-simple's per-logger level property for Jetty. */
    static final String JETTY_SIMPLE_LOGGER_LEVEL = "org.slf4j.simpleLogger.log.org.eclipse.jetty";

    /**
     * Keep the {@code Authorization} header out of the logs: cap Jetty's loggers at {@code info}.
     *
     * <p>Jetty's {@code DEBUG} wire trace ({@code org.eclipse.jetty.http.HttpParser} and friends)
     * prints every request header verbatim, so a worker run with debug logging would write each
     * caller's Pass to its log. The SDK itself never logs the header. This sets slf4j-simple's
     * per-logger property, which it reads when each logger is created, so it must run before the
     * server starts. A worker that ships another slf4j binding (logback, log4j) must keep
     * {@code org.eclipse.jetty} at {@code INFO} or above in that binding's configuration.
     */
    static void quietWireLogging() {
        System.setProperty(JETTY_SIMPLE_LOGGER_LEVEL, "info");
    }

    /**
     * The refusal for a transport that carries no Pass.
     *
     * @param transport the transport flag or name (e.g. {@code --tcp}, {@code stdin/stdout})
     * @return the exception to throw
     */
    static RefusedException transportRefusal(String transport) {
        return refusal("only --http is allowed; " + transport + " carries no Pass.");
    }
}
