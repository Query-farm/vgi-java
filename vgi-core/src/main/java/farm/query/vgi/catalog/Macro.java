// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.catalog;

import java.util.List;
import java.util.Map;

/**
 * A SQL macro exposed in the catalog. {@code parameterDefaults} is an
 * optional ordered map of parameter-name → SQL expression (used when the
 * macro has named-with-default parameters).
 */
public record Macro(String schema, String name, MacroType macroType,
                     List<String> parameters,
                     Map<String, String> parameterDefaults,
                     String definition, String comment, Map<String, String> tags) {
    public Macro(String schema, String name, MacroType macroType,
                  List<String> parameters, String definition, String comment) {
        this(schema, name, macroType, parameters, Map.of(), definition, comment, Map.of());
    }

    public Macro(String schema, String name, MacroType macroType,
                  List<String> parameters, Map<String, String> parameterDefaults,
                  String definition, String comment) {
        this(schema, name, macroType, parameters, parameterDefaults, definition, comment, Map.of());
    }
}
