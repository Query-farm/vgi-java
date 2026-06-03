// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Declares a DuckDB secret type backed by this worker. Mirrors vgi-go
 * {@code vgi.SecretTypeSpec}.
 *
 * <p>Secret types are advertised at attach time via
 * {@code CatalogAttachResult.secret_types}. Mark sensitive fields in
 * {@code parametersSchema} with custom field metadata {@code "redact":"true"}
 * so DuckDB can mask them in {@code duckdb_secrets()}.
 *
 * @param name             secret-type name registered with DuckDB
 * @param description       human-readable description for introspection
 * @param parametersSchema Arrow schema of the secret's parameter fields
 */
public record SecretTypeSpec(String name, String description, Schema parametersSchema) {}
