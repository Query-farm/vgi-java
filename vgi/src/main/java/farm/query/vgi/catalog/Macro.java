// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import java.util.List;
import java.util.Map;

/**
 * A SQL macro exposed in the catalog. {@code parameterDefaults} is an
 * optional ordered map of parameter-name → SQL expression (used when the
 * macro has named-with-default parameters).
 *
 * @param schema            owning schema name
 * @param name              macro name
 * @param macroType         whether the macro is scalar or table-valued
 * @param parameters        positional parameter names
 * @param parameterDefaults ordered parameter-name → default SQL expression map
 * @param definition        the macro body SQL expanded by DuckDB
 * @param comment           free-text macro comment
 * @param tags              arbitrary key/value macro metadata
 */
public record Macro(String schema, String name, MacroType macroType,
                     List<String> parameters,
                     Map<String, String> parameterDefaults,
                     String definition, String comment, Map<String, String> tags) {
    /**
     * Builds a macro with no parameter defaults and no tags.
     *
     * @param schema     owning schema name
     * @param name       macro name
     * @param macroType  scalar or table macro
     * @param parameters positional parameter names
     * @param definition the macro body SQL
     * @param comment    macro comment
     */
    public Macro(String schema, String name, MacroType macroType,
                  List<String> parameters, String definition, String comment) {
        this(schema, name, macroType, parameters, Map.of(), definition, comment, Map.of());
    }

    /**
     * Builds a macro with parameter defaults but no tags.
     *
     * @param schema            owning schema name
     * @param name              macro name
     * @param macroType         scalar or table macro
     * @param parameters        positional parameter names
     * @param parameterDefaults parameter-name → default SQL expression map
     * @param definition        the macro body SQL
     * @param comment           macro comment
     */
    public Macro(String schema, String name, MacroType macroType,
                  List<String> parameters, Map<String, String> parameterDefaults,
                  String definition, String comment) {
        this(schema, name, macroType, parameters, parameterDefaults, definition, comment, Map.of());
    }
}
