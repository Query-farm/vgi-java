// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * A secret requirement declared by a function, embedded in {@link FunctionInfo#required_secrets}.
 *
 * <p>The extension resolves each requirement before the first bind and sends
 * every secret it found, keyed by the secret's unique DuckDB name (not its
 * type). Select the delivered secrets with {@code farm.query.vgi.Secrets}'
 * type/name/scope selectors.</p>
 *
 * <p>Declare one with the factories: {@link #ofType(String)} (the simplest
 * form, "a secret of type T"), {@link #scoped(String, String)} or
 * {@link #named(String, String)}. A blank {@code secret_type} is rejected when
 * the requirement is built, so a malformed declaration fails loudly instead of
 * never requesting its secret.</p>
 *
 * @param secret_type the secret type/provider the function needs; must not be blank.
 * @param scope       optional scope narrowing which secret applies, or {@code null}.
 * @param secret_name optional explicit secret name, or {@code null} to resolve by type/scope.
 */
public record FunctionRequiredSecret(
        String secret_type,
        @Nullable String scope,
        @Nullable String secret_name) implements ArrowSerializableRecord {

    /**
     * Validates the declaration.
     *
     * @throws IllegalArgumentException when {@code secret_type} is null or blank
     */
    public FunctionRequiredSecret {
        if (secret_type == null || secret_type.isBlank()) {
            throw new IllegalArgumentException(
                    "FunctionRequiredSecret.secret_type must name a DuckDB secret type, got "
                            + (secret_type == null ? "null" : "'" + secret_type + "'"));
        }
    }

    /**
     * A secret of {@code secretType} with no scope and no name: the simplest form.
     * DuckDB resolves an empty-scope lookup to an unscoped secret of the type.
     *
     * @param secretType the DuckDB secret type (e.g. {@code "s3"})
     * @return the requirement
     */
    public static FunctionRequiredSecret ofType(String secretType) {
        return new FunctionRequiredSecret(secretType, null, null);
    }

    /**
     * A secret of {@code secretType} whose scope best matches {@code scope}
     * (DuckDB's longest-prefix rule).
     *
     * @param secretType the DuckDB secret type
     * @param scope the path to match against secret scopes (e.g. {@code s3://bucket/})
     * @return the requirement
     */
    public static FunctionRequiredSecret scoped(String secretType, String scope) {
        return new FunctionRequiredSecret(secretType, scope, null);
    }

    /**
     * The secret named {@code secretName}, delivered only if it is of {@code secretType}.
     *
     * @param secretType the DuckDB secret type the secret must have
     * @param secretName the secret's unique DuckDB name
     * @return the requirement
     */
    public static FunctionRequiredSecret named(String secretType, String secretName) {
        return new FunctionRequiredSecret(secretType, null, secretName);
    }
}
