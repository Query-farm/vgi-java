// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.example.table.SecretCacheNonceFunction;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code secret_cached_scalar(value BIGINT) -> VARCHAR} — labels every row
 * {@code '<secret_string>|<nonce>'}, memoized per value per secret. With no
 * {@code vgi_example} secret resolved the label is {@code '|<nonce>'}; a
 * dropped secret is a state {@code cache/secret_scope.test} drives, so it must
 * not be an error.
 *
 * <p>The scalar path of the secret-fingerprint cache (see
 * {@link SecretCacheNonceFunction}): the extension keys the per-value memo on
 * the fingerprint of the execute-time bind's secrets. The secret is requested
 * through the two-phase bind, as {@link ReturnSecretValueFunction} does.
 *
 * <p>One nonce per {@code process} call, shared by the whole batch, so a
 * served value keeps the nonce of the call that produced it. {@code per_value}
 * is a TEST choice, as on {@code cached_double_scalar}: the point is coverage
 * of the tier, not economics. Mirrors vgi-python's
 * {@code SecretCachedScalarFunction}.
 */
public final class SecretCachedScalarFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.UTF8);

    private static final CacheControl CACHE_CONTROL =
            CacheControl.builder().ttl(300).perValue(true).build();

    private static final FunctionSpec SPEC = FunctionSpec.builder("secret_cached_scalar")
            .metadata(FunctionMetadata.describe(
                    "Returns '<secret_string>|<nonce>' per value; memoized per value per secret"))
            .arg("value", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public CacheControl cacheControl() { return CACHE_CONTROL; }

    @Override public BindResponse onBind(ScalarBindParams p) {
        if (!p.resolvedSecretsProvided()) {
            return new BindResponse(OUTPUT_SCHEMA_IPC, new byte[0],
                    List.of(SecretCacheNonceFunction.SECRET_TYPE), List.of(""), List.of(""));
        }
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input,
                                                 BufferAllocator alloc) {
        String secret = SecretCacheNonceFunction.secretString(params.secrets());
        Text label = new Text((secret == null ? "" : secret) + "|" + SecretCacheNonceFunction.nonce());
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        VarCharVector v = (VarCharVector) out.getVector("result");
        for (int i = 0; i < rows; i++) v.setSafe(i, label);
        out.setRowCount(rows);
        return out;
    }
}
