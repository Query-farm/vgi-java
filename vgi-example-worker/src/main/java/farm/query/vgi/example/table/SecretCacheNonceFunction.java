// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.Secrets;
import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;

/**
 * {@code secret_cache_nonce() -> (secret_string VARCHAR, nonce BIGINT)} — one
 * row carrying the {@code vgi_example} secret's {@code secret_string} (NULL when
 * no secret resolves) and a nonce minted per real invocation; cacheable.
 *
 * <p>The extension keys a secret-dependent result on a fingerprint of the
 * secrets its bind resolved, never their values: an unchanged secret HITs, and
 * a rotated, re-fielded or dropped one MISSes. This fixture and its two
 * siblings ({@code SecretCachedScalarFunction},
 * {@code SecretCachedLateralFunction}) give that fingerprint one fixture per
 * cache path it has to reach. This one covers the producer path with a
 * <em>declared</em> secret ({@link #requiredSecrets()}), both as a function
 * call and as the function-backed {@code data.secret_cache_nonce} table. Backs
 * {@code cache/secret_scope.test}; mirrors vgi-python's
 * {@code SecretCacheNonceFunction}.
 *
 * <p>The nonce is random rather than a counter like {@link CacheFunctions}'s:
 * a pooled worker may run several processes, and per-process counters repeat
 * across them. Equal nonces then prove a HIT and different ones a MISS on any
 * pool size.
 */
public final class SecretCacheNonceFunction extends SimpleTableFunction {

    /** The secret type all three secret-cache fixtures read. */
    public static final String SECRET_TYPE = "vgi_example";

    private static final Schema OUTPUT = Schemas.of(
            Schemas.nullable("secret_string", Schemas.UTF8),
            Schemas.nullable("nonce", Schemas.INT64));

    private static final SecureRandom RNG = new SecureRandom();

    private static final FunctionSpec SPEC = FunctionSpec.builder("secret_cache_nonce")
            .metadata(FunctionMetadata.describe(
                            "One row with a secret's value and a per-invocation nonce; cacheable per secret")
                    .withCategories("generator", "cache", "secret", "testing"))
            .build();

    /**
     * A value unique to one invocation across every process of a worker pool:
     * 56 random bits, so it is never negative.
     *
     * @return a fresh non-negative nonce
     */
    public static long nonce() {
        return RNG.nextLong() >>> 8;
    }

    /**
     * The first resolved {@code vgi_example} secret's {@code secret_string}.
     *
     * @param secretsIpc the resolved-secrets IPC blob, or {@code null}
     * @return the value, or {@code null} when no such secret resolved
     */
    public static String secretString(byte[] secretsIpc) {
        List<Map<String, String>> matches = Secrets.parse(secretsIpc).ofType(SECRET_TYPE);
        return matches.isEmpty() ? null : matches.get(0).get("secret_string");
    }

    @Override public FunctionSpec spec() { return SPEC; }
    @Override protected Schema outputSchema() { return OUTPUT; }

    @Override public List<FunctionRequiredSecret> requiredSecrets() {
        return List.of(new FunctionRequiredSecret(SECRET_TYPE, null, null));
    }

    /** Reached only on a cache MISS, so the nonce holds steady across HITs. */
    @Override public TableProducerState createProducer(TableInitParams p) {
        return new State(secretString(p.secrets()), nonce());
    }

    /** The one row to emit. Public fields + no-arg ctor for the HTTP state token. */
    public static final class State extends TableProducerState {
        /** The secret's {@code secret_string}, or {@code null} when none resolved. */
        public String secretString;
        /** The per-invocation nonce. */
        public long nonce;
        /** Whether the single row has been emitted. */
        public boolean done;

        /** Required no-arg constructor for state deserialization. */
        public State() {}

        State(String secretString, long nonce) {
            this.secretString = secretString;
            this.nonce = nonce;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            String s = secretString;
            long n = nonce;
            BatchUtil.emit(OUTPUT, 1, out,
                    CacheControl.ttl(CacheFunctions.DEFAULT_TTL_SECONDS).toMetadata(),
                    (root, rows, ignored) -> {
                        VarCharVector sv = (VarCharVector) root.getVector("secret_string");
                        if (s == null) sv.setNull(0);
                        else sv.setSafe(0, new Text(s));
                        ((BigIntVector) root.getVector("nonce")).setSafe(0, n);
                    });
        }
    }
}
