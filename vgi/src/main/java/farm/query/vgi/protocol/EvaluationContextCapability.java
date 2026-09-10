// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.nio.charset.StandardCharsets;

/** One immutable filter evaluation-context profile implemented by a worker. */
public record EvaluationContextCapability(
        String profile,
        @Nullable String provider_fingerprint) implements ArrowSerializableRecord {

    public EvaluationContextCapability {
        if (!"vgi.duckdb.session.v1".equals(profile)) {
            throw new IllegalArgumentException("unknown filter evaluation-context profile: " + profile);
        }
        if (provider_fingerprint != null
                && (provider_fingerprint.isEmpty()
                        || provider_fingerprint.getBytes(StandardCharsets.UTF_8).length > 256)) {
            throw new IllegalArgumentException(
                    "provider_fingerprint must be nonempty and at most 256 UTF-8 bytes");
        }
    }

    public EvaluationContextCapability(String profile) {
        this(profile, null);
    }
}
