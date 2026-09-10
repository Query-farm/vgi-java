// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The unprojected bind-output column list, used to create exact v2 references.
 *
 * <p>A v2 {@code column_index} is always the position in the function's full
 * bind output, before projection. Building this helper from that schema keeps
 * both the authoritative index and name together.
 *
 * <pre>{@code
 * // The scan projects two of the table's columns, in this order.
 * ProjectedColumns cols = ProjectedColumns.of(List.of("n", "name"));
 *
 * EncodedPushdownFilters f = PushdownFiltersEncoder.builder()
 *         .filter(cols.column("n"), FilterPredicate.ge(5L))
 *         .filter(cols.column("name"), FilterPredicate.isNotNull())
 *         .encode();
 * }</pre>
 *
 * <p>Instances are immutable.
 */
public final class ProjectedColumns {

    private final List<ProjectedColumn> columns;
    private final Map<String, ProjectedColumn> byName;

    private ProjectedColumns(List<ProjectedColumn> columns, Map<String, ProjectedColumn> byName) {
        this.columns = columns;
        this.byName = byName;
    }

    /**
     * Build from the projected column names, in projection order.
     *
     * @param projectedColumnNames the names of the columns this scan projects,
     *                             in the order they are projected
     * @return the projected column list
     * @throws IllegalArgumentException if a name repeats (an index could not
     *         then be resolved unambiguously by name)
     */
    public static ProjectedColumns of(List<String> projectedColumnNames) {
        List<ProjectedColumn> cols = new ArrayList<>(projectedColumnNames.size());
        Map<String, ProjectedColumn> index = new LinkedHashMap<>();
        for (int i = 0; i < projectedColumnNames.size(); i++) {
            ProjectedColumn c = ProjectedColumn.of(projectedColumnNames.get(i), i);
            cols.add(c);
            if (index.put(c.name(), c) != null) {
                throw new IllegalArgumentException(
                        "duplicate projected column name '" + c.name()
                                + "' — resolve columns by index instead");
            }
        }
        return new ProjectedColumns(List.copyOf(cols), Map.copyOf(index));
    }

    /**
     * Build from a projected schema — e.g. the bind response's output schema
     * narrowed to the projection.
     *
     * @param projectedSchema the schema of the projected columns, in projection order
     * @return the projected column list
     */
    public static ProjectedColumns of(Schema projectedSchema) {
        List<String> names = new ArrayList<>(projectedSchema.getFields().size());
        for (Field f : projectedSchema.getFields()) names.add(f.getName());
        return of(names);
    }

    /**
     * Look up a projected column by name.
     *
     * @param name the column name
     * @return the column, carrying its projected index
     * @throws IllegalArgumentException if the projection does not contain {@code name}
     */
    public ProjectedColumn column(String name) {
        ProjectedColumn c = byName.get(name);
        if (c == null) {
            throw new IllegalArgumentException(
                    "column '" + name + "' is not in the projection " + byName.keySet());
        }
        return c;
    }

    /**
     * Look up a projected column by its projected index.
     *
     * @param projectedIndex the position in the projected column list
     * @return the column
     * @throws IndexOutOfBoundsException if the index is outside the projection
     */
    public ProjectedColumn column(int projectedIndex) {
        return columns.get(projectedIndex);
    }

    /**
     * All projected columns, in projection order.
     *
     * @return an immutable list of the projected columns
     */
    public List<ProjectedColumn> all() {
        return columns;
    }
}
