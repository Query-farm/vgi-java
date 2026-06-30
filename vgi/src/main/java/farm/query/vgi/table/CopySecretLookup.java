// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

/**
 * A single secret to resolve via the two-phase secret bind, returned from a COPY
 * format's secret-bind hook ({@link CopyToFunction#secretLookups} /
 * {@link CopyFromFunction#secretLookups}). Mirrors a vgi-python
 * {@code SecretLookupEntry}.
 *
 * @param secretType the DuckDB secret type to resolve (e.g. {@code "s3"},
 *     {@code "vgi_example"}).
 * @param scope optional scope for longest-prefix matching — typically the COPY
 *     path; {@code null} requests an unscoped lookup.
 * @param name optional secret name for name-based lookup; {@code null} for none.
 */
public record CopySecretLookup(String secretType, String scope, String name) {

    /** A scoped lookup with no explicit secret name. */
    public static CopySecretLookup scoped(String secretType, String scope) {
        return new CopySecretLookup(secretType, scope, null);
    }
}
