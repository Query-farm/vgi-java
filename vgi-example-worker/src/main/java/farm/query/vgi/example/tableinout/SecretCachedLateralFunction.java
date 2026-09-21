// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.example.table.SecretCacheNonceFunction;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.tableinout.RowTransformFunction;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code secret_cached_lateral(x BIGINT) -> (secret_string VARCHAR, nonce BIGINT)}
 * — blended 1-&gt;1 map: every output row carries the {@code vgi_example}
 * secret's {@code secret_string} (NULL when none resolves) and the nonce of the
 * call that produced it; the input value is ignored.
 *
 * <p>The LATERAL path of the secret-fingerprint cache (see
 * {@link SecretCacheNonceFunction}). Unlike the producer, the secret is
 * <em>requested</em> from {@link #onBind} — the two-phase bind — so the
 * extension only learns of the dependency mid-bind, and the fingerprint must
 * cover what the retry resolved. Advertises {@code ttl} + {@code per_value} on
 * every output batch, so a correlated {@code LATERAL} call is memoized per input
 * value per secret ({@code per_value} is a TEST choice, as on
 * {@code cached_double}). Backs {@code cache/secret_scope.test}; mirrors
 * vgi-python's {@code SecretCachedLateralFunction}.
 */
public final class SecretCachedLateralFunction implements RowTransformFunction {

    private static final Schema OUTPUT = Schemas.of(
            Schemas.nullable("secret_string", Schemas.UTF8),
            Schemas.nullable("nonce", Schemas.INT64));
    private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

    private static final FunctionSpec SPEC = FunctionSpec.builder("secret_cached_lateral")
            .metadata(FunctionMetadata.describe(
                            "Blended map emitting a secret's value and a per-call nonce; memoized per secret")
                    .withCategories("blended", "cache", "secret", "test"))
            .arg("x", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        if (!params.resolvedSecretsProvided()) {
            return new BindResponse(OUTPUT_IPC, new byte[0],
                    List.of(SecretCacheNonceFunction.SECRET_TYPE), List.of(""), List.of(""));
        }
        return BindResponse.forSchema(OUTPUT_IPC);
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new State(SecretCacheNonceFunction.secretString(params.secrets()));
    }

    /** Named + public fields + no-arg ctor for the HTTP state-token round-trip. */
    public static final class State extends TableInOutExchangeState {
        /** The secret's {@code secret_string}, or {@code null} when none resolved. */
        public String secretString;

        /** No-arg constructor for HTTP state-token deserialization. */
        public State() {}

        State(String secretString) { this.secretString = secretString; }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            int rows = input.root().getRowCount();
            long nonce = SecretCacheNonceFunction.nonce();
            Text secret = secretString == null ? null : new Text(secretString);
            VectorSchemaRoot outRoot = VectorSchemaRoot.create(OUTPUT, Allocators.root());
            outRoot.allocateNew();
            VarCharVector secrets = (VarCharVector) outRoot.getVector("secret_string");
            BigIntVector nonces = (BigIntVector) outRoot.getVector("nonce");
            for (int i = 0; i < rows; i++) {
                if (secret == null) secrets.setNull(i);
                else secrets.setSafe(i, secret);
                nonces.setSafe(i, nonce);
            }
            outRoot.setRowCount(rows);
            out.emit(outRoot, CacheControl.builder().ttl(300).perValue(true).build().toMetadata());
        }
    }
}
