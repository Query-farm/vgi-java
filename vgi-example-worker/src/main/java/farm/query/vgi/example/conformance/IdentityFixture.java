// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.conformance;

import farm.query.vgi.Worker;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthUnavailableException;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.identity.GrantRefusedError;
import farm.query.vgirpc.identity.IdentityUnavailableError;
import farm.query.vgirpc.identity.IssuedGrant;
import farm.query.vgirpc.identity.TokenIdentity;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Opt the HTTP fixture into {@code vgi_rpc.Identity.v1} with the vgi-rpc conformance policy.
 *
 * <p>Enabled by {@code --identity} (or {@code VGI_FIXTURE_IDENTITY=1}) so the cross-SDK
 * hosted-protocols group can run its identity cases with
 * {@code vgi-rpc-test-hosted --url ... --identity}. Off by default, so the DuckDB integration
 * runs see the same worker as before.
 *
 * <p>The policy is pinned by vgi-rpc's {@code IDENTITY_CONFORMANCE_FIXTURE.md}; every value below
 * is the one that document names. Both hooks are pure functions of their arguments.
 *
 * <p><strong>The header authenticator is trivially spoofable</strong> by anyone who can reach the
 * port. It is a test fixture and must never be deployed.
 */
public final class IdentityFixture {

    /** The one principal permitted to introspect. */
    public static final String INTROSPECTOR = "conformance-introspector";

    private static final String PRINCIPAL_HEADER = "X-Conformance-Principal";
    private static final String AUTH_TIME_HEADER = "X-Conformance-Auth-Time";
    private static final String SUBJECT = "subject@conformance.example";
    private static final String SUBJECT_TOKEN_NAME = "conformance-subject";
    private static final long SUBJECT_TTL = 300;

    private static final String TOKEN_UNAVAILABLE = "conformance-unavailable-token";
    private static final int UNAVAILABLE_RETRY_AFTER = 5;
    /** Answered with the transport-auth unavailable error, which the framework must translate. */
    private static final String TOKEN_AUTH_UNAVAILABLE = "conformance-auth-unavailable-token";
    private static final int AUTH_UNAVAILABLE_RETRY_AFTER = 7;
    private static final String TOKEN_UNKNOWN = "conformance-unknown-token";
    private static final String TOKEN_ZERO_TTL = "conformance-zero-ttl-token";
    private static final String TOKEN_MINIMAL = "conformance-minimal-token";
    private static final String TOKEN_PADDED_PROBE = "  conformance-padded-probe  ";
    private static final String TOKEN_PADDED_NAME = "conformance-padded";

    private static final double MAX_AUTH_AGE = 900.0;
    private static final String GRANT_TOKEN_PREFIX = "conformance-grant-for:";
    private static final String SCOPE_SEPARATOR = "|";
    private static final double GRANT_EXPIRES_AT = 1893456000.0;
    private static final String GRANT_ID = "conformance-grant-id";
    private static final String REFUSED_PURPOSE = "conformance-refused";
    private static final String MINIMAL_PURPOSE = "conformance-minimal";
    private static final String AUTH_UNAVAILABLE_PURPOSE = "conformance-auth-unavailable";

    private IdentityFixture() {}

    /**
     * Install the conformance hooks and allowlist on {@code worker}.
     *
     * @param worker the fixture worker
     * @return the same worker
     */
    public static Worker apply(Worker worker) {
        return worker.resolveToken(IdentityFixture::resolveToken)
                .mintGrant(IdentityFixture::mintGrant)
                .introspectPrincipals(INTROSPECTOR)
                .maxAuthAge(MAX_AUTH_AGE);
    }

    /**
     * Derive the caller from {@code X-Conformance-Principal} / {@code X-Conformance-Auth-Time}. An
     * absent principal header means unauthenticated; the auth time is carried verbatim, because
     * the guard does the parsing.
     *
     * @return the spoofable test authenticator
     */
    public static Authenticator authenticator() {
        return request -> {
            String principal = request.getHeader(PRINCIPAL_HEADER);
            if (principal == null || principal.isEmpty()) return AuthContext.ANONYMOUS;
            String authTime = request.getHeader(AUTH_TIME_HEADER);
            Map<String, Object> claims = authTime == null
                    ? Collections.emptyMap() : Map.of("auth_time", authTime);
            return new AuthContext("conformance", true, principal, claims);
        };
    }

    static TokenIdentity resolveToken(String token) {
        if (TOKEN_UNAVAILABLE.equals(token)) {
            throw new IdentityUnavailableError("conformance: mapping store unreachable",
                    UNAVAILABLE_RETRY_AFTER, null);
        }
        if (TOKEN_AUTH_UNAVAILABLE.equals(token)) {
            // The transport-auth spelling of the same outage -- what a hook calling the store an
            // authenticator calls raises. Translated by the framework, never here.
            throw new AuthUnavailableException("conformance: authority unreachable",
                    AUTH_UNAVAILABLE_RETRY_AFTER, null);
        }
        if (TOKEN_UNKNOWN.equals(token)) return null;
        if (TOKEN_ZERO_TTL.equals(token)) return new TokenIdentity(SUBJECT, SUBJECT_TOKEN_NAME, 0);
        if (TOKEN_MINIMAL.equals(token)) return new TokenIdentity(SUBJECT);
        if (TOKEN_PADDED_PROBE.equals(token)) {
            return new TokenIdentity(SUBJECT, TOKEN_PADDED_NAME, SUBJECT_TTL);
        }
        return new TokenIdentity(SUBJECT, SUBJECT_TOKEN_NAME, SUBJECT_TTL);
    }

    static IssuedGrant mintGrant(String principal, String purpose, List<String> scopes, long ttlSeconds) {
        if (AUTH_UNAVAILABLE_PURPOSE.equals(purpose)) {
            throw new AuthUnavailableException("conformance: grant store unreachable",
                    AUTH_UNAVAILABLE_RETRY_AFTER, null);
        }
        if (REFUSED_PURPOSE.equals(purpose)) {
            throw new GrantRefusedError("conformance: this purpose is refused");
        }
        String token = GRANT_TOKEN_PREFIX + principal + SCOPE_SEPARATOR + String.join(",", scopes);
        if (MINIMAL_PURPOSE.equals(purpose)) return new IssuedGrant(token, GRANT_EXPIRES_AT);
        return new IssuedGrant(token, GRANT_EXPIRES_AT, GRANT_ID);
    }
}
