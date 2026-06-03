// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import java.util.Map;

/**
 * A SQL view exposed in the catalog. {@code schema} is the schema name;
 * {@code definition} is a SQL query string evaluated by DuckDB.
 *
 * @param schema         owning schema name
 * @param name           view name
 * @param definition     SQL query string evaluated by DuckDB
 * @param comment        free-text view comment
 * @param tags           arbitrary key/value view metadata
 * @param columnComments per-column comments keyed by column name
 */
public record View(String schema, String name, String definition,
                    String comment, Map<String, String> tags,
                    Map<String, String> columnComments) {
    /**
     * Builds a view with no tags and no column comments.
     *
     * @param schema     owning schema name
     * @param name       view name
     * @param definition SQL query string
     * @param comment    view comment
     */
    public View(String schema, String name, String definition, String comment) {
        this(schema, name, definition, comment, Map.of(), Map.of());
    }

    /**
     * Builds a view with tags but no column comments.
     *
     * @param schema     owning schema name
     * @param name       view name
     * @param definition SQL query string
     * @param comment    view comment
     * @param tags       view metadata tags
     */
    public View(String schema, String name, String definition, String comment,
                 Map<String, String> tags) {
        this(schema, name, definition, comment, tags, Map.of());
    }

    /**
     * Returns a copy with the given per-column comment map (name → comment).
     *
     * @param columnComments per-column comments keyed by column name
     * @return a new view with the column comments applied
     */
    public View withColumnComments(Map<String, String> columnComments) {
        return new View(schema, name, definition, comment, tags, columnComments);
    }
}
