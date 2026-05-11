// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.catalog;

import java.util.Map;

/**
 * A SQL view exposed in the catalog. {@code schema} is the schema name;
 * {@code definition} is a SQL query string evaluated by DuckDB.
 */
public record View(String schema, String name, String definition,
                    String comment, Map<String, String> tags) {
    public View(String schema, String name, String definition, String comment) {
        this(schema, name, definition, comment, Map.of());
    }
}
