// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * A secret requirement declared by a function, embedded in {@link FunctionInfo#required_secrets}.
 *
 * @param secret_type the secret type/provider the function needs.
 * @param scope       optional scope narrowing which secret applies, or {@code null}.
 * @param secret_name optional explicit secret name, or {@code null} to resolve by type/scope.
 */
public record FunctionRequiredSecret(
        String secret_type,
        @Nullable String scope,
        @Nullable String secret_name) implements ArrowSerializableRecord {}
