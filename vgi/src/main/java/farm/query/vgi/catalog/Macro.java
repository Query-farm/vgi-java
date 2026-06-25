// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import java.util.List;
import java.util.Map;

/**
 * A SQL macro exposed in the catalog. {@code parameterDefaults} is an
 * optional ordered map of parameter-name → SQL expression (used when the
 * macro has named-with-default parameters). {@code parameterDocs} is an
 * optional parameter-name → description map: each documented parameter's
 * description flows over the wire via the macro {@code arguments_schema}'s
 * {@code vgi_doc} field metadata (the same channel functions use for
 * per-argument docs), so the DuckDB extension's {@code vgi_function_arguments()}
 * can surface it. Keys must appear in {@code parameters}.
 *
 * @param schema            owning schema name
 * @param name              macro name
 * @param macroType         whether the macro is scalar or table-valued
 * @param parameters        positional parameter names
 * @param parameterDefaults ordered parameter-name → default SQL expression map
 * @param parameterDocs     parameter-name → description map (per-parameter docs)
 * @param definition        the macro body SQL expanded by DuckDB
 * @param comment           free-text macro comment
 * @param tags              arbitrary key/value macro metadata
 */
public record Macro(String schema, String name, MacroType macroType,
                     List<String> parameters,
                     Map<String, String> parameterDefaults,
                     Map<String, String> parameterDocs,
                     String definition, String comment, Map<String, String> tags) {

    /**
     * Canonical constructor: validates that every documented parameter name
     * appears in {@code parameters}.
     *
     * @param schema            owning schema name
     * @param name              macro name
     * @param macroType         scalar or table macro
     * @param parameters        positional parameter names
     * @param parameterDefaults parameter-name → default SQL expression map
     * @param parameterDocs     parameter-name → description map
     * @param definition        the macro body SQL
     * @param comment           macro comment
     * @param tags              arbitrary key/value macro metadata
     */
    public Macro {
        if (parameterDocs != null && parameters != null) {
            for (String docName : parameterDocs.keySet()) {
                if (!parameters.contains(docName)) {
                    throw new IllegalArgumentException("Macro '" + name
                            + "': documented parameter '" + docName
                            + "' not found in parameters list " + parameters);
                }
            }
        }
    }

    /**
     * Builds a macro with no parameter defaults, docs, or tags.
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
        this(schema, name, macroType, parameters, Map.of(), Map.of(), definition, comment, Map.of());
    }

    /**
     * Builds a macro with parameter defaults but no docs or tags.
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
        this(schema, name, macroType, parameters, parameterDefaults, Map.of(), definition, comment, Map.of());
    }

    /**
     * Builds a macro with parameter defaults and tags but no docs.
     *
     * @param schema            owning schema name
     * @param name              macro name
     * @param macroType         scalar or table macro
     * @param parameters        positional parameter names
     * @param parameterDefaults parameter-name → default SQL expression map
     * @param definition        the macro body SQL
     * @param comment           macro comment
     * @param tags              arbitrary key/value macro metadata
     */
    public Macro(String schema, String name, MacroType macroType,
                  List<String> parameters, Map<String, String> parameterDefaults,
                  String definition, String comment, Map<String, String> tags) {
        this(schema, name, macroType, parameters, parameterDefaults, Map.of(), definition, comment, tags);
    }
}
