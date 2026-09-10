// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

/**
 * The column a pushdown filter targets, identified the way the VGI wire
 * identifies it: by name and by position in the unprojected bind-output schema.
 *
 * <p><strong>{@code bindIndex} is not the projected position.</strong> Filter
 * references are resolved before projection, against the function's complete
 * bind output. Consumers may fall back to an unambiguous name when evaluating
 * a projected batch, but the wire index remains the bind-output index.
 *
 * <p>Because that mistake is invisible at runtime, prefer building columns
 * through {@link ProjectedColumns}, built from the complete bind schema:
 *
 * <pre>{@code
 * ProjectedColumns cols = ProjectedColumns.of(bindOutputSchema);
 * ProjectedColumn n = cols.column("n");                               // index 0
 * }</pre>
 *
 * <p>Use {@link #of(String, int)} directly only when the bind-output index is known.
 *
 * @param name           the column name, as the worker knows it (used for
 *                       join-key matching, which is by name)
 * @param bindIndex the column's zero-based position in the unprojected bind-output schema
 */
public record ProjectedColumn(String name, int bindIndex) {

    /** Validates that the name is present and the index is a plausible position. */
    public ProjectedColumn {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("ProjectedColumn requires a non-empty name");
        }
        if (bindIndex < 0) {
            throw new IllegalArgumentException(
                    "ProjectedColumn.bindIndex must be >= 0, got " + bindIndex);
        }
    }

    /**
     * A column at a known bind-output position.
     *
     * @param name           the column name
     * @param projectedIndex the bind-output position (legacy parameter name)
     * @return the column reference
     */
    public static ProjectedColumn of(String name, int projectedIndex) {
        return new ProjectedColumn(name, projectedIndex);
    }

    /** @deprecated VGI 2.0 column indices address the unprojected bind-output schema. */
    @Deprecated(forRemoval = true)
    public int projectedIndex() {
        return bindIndex;
    }
}
