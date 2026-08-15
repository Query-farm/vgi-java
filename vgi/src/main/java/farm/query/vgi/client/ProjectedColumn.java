// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

/**
 * The column a pushdown filter targets, identified the way the VGI wire
 * identifies it: by name <em>and</em> by position in the <strong>projected</strong>
 * column list.
 *
 * <p><strong>{@code projectedIndex} is not the base-schema position.</strong> It
 * is the column's index in the projection the client asked for — the same list
 * it sends as {@code InitRequest.projection_ids} and the same order the worker
 * emits its batches in. A worker applies a constant or {@code IN} filter by
 * index ({@code batch.column(column_index)}), so an index taken from the full
 * table schema silently filters the <em>wrong column</em> whenever a projection
 * drops or reorders columns: no error, just wrong rows.
 *
 * <p>Because that mistake is invisible at runtime, prefer building columns
 * through {@link ProjectedColumns}, which derives the index from the projected
 * column list itself and cannot drift:
 *
 * <pre>{@code
 * ProjectedColumns cols = ProjectedColumns.of(List.of("n", "name"));  // the projection
 * ProjectedColumn n = cols.column("n");                               // index 0
 * }</pre>
 *
 * <p>Use {@link #of(String, int)} directly only when the index is already known
 * to be a projected position.
 *
 * @param name           the column name, as the worker knows it (used for
 *                       join-key matching, which is by name)
 * @param projectedIndex the column's zero-based position in the projected
 *                       column list, <em>not</em> in the base schema
 */
public record ProjectedColumn(String name, int projectedIndex) {

    /** Validates that the name is present and the index is a plausible position. */
    public ProjectedColumn {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("ProjectedColumn requires a non-empty name");
        }
        if (projectedIndex < 0) {
            throw new IllegalArgumentException(
                    "ProjectedColumn.projectedIndex must be >= 0, got " + projectedIndex);
        }
    }

    /**
     * A column at a known <em>projected</em> position.
     *
     * @param name           the column name
     * @param projectedIndex the position in the projected column list (see the class doc)
     * @return the column reference
     */
    public static ProjectedColumn of(String name, int projectedIndex) {
        return new ProjectedColumn(name, projectedIndex);
    }
}
