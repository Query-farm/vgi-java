// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.catalog;

import java.util.Map;

/**
 * A SQL view exposed in the catalog. {@code schema} is the schema name;
 * {@code definition} is a SQL query string evaluated by DuckDB.
 */
public record View(String schema, String name, String definition,
                    String comment, Map<String, String> tags,
                    Map<String, String> columnComments) {
    public View(String schema, String name, String definition, String comment) {
        this(schema, name, definition, comment, Map.of(), Map.of());
    }

    public View(String schema, String name, String definition, String comment,
                 Map<String, String> tags) {
        this(schema, name, definition, comment, tags, Map.of());
    }

    /** Same view with the given per-column comment map (name → comment). */
    public View withColumnComments(Map<String, String> columnComments) {
        return new View(schema, name, definition, comment, tags, columnComments);
    }
}
