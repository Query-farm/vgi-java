// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Build / parse a macro {@code arguments_schema}.
 *
 * <p>Mirrors {@code vgi-python/vgi/argument_spec.py macro_arguments_schema}: one
 * Arrow {@link Field} per macro parameter, in {@code parameters} order, each
 * nullable. A parameter's field type is the type of its default value when one
 * is known (inferred from the default-value SQL literal, via
 * {@link MacroDefaultsEncoder}), else {@link ArrowType.Null}. Each documented
 * parameter's description rides as field metadata under the same
 * {@code vgi_doc} key functions use for per-argument docs (UTF-8,
 * presence-only — omitted when empty).
 */
public final class MacroArgumentsSchema {

    private MacroArgumentsSchema() {}

    /** Field-metadata key carrying a parameter's description (shared with functions). */
    static final String VGI_DOC_KEY = "vgi_doc";

    /**
     * Build a macro {@code arguments_schema}.
     *
     * @param parameters        ordered macro parameter names
     * @param parameterDefaults parameter-name → default-value SQL literal map (used to type fields); may be {@code null}/empty
     * @param parameterDocs     parameter-name → description map; documented params get {@code vgi_doc} metadata; may be {@code null}/empty
     * @return one nullable {@link Field} per parameter, in order
     */
    public static Schema build(List<String> parameters,
                               Map<String, String> parameterDefaults,
                               Map<String, String> parameterDocs) {
        Map<String, String> docs = parameterDocs == null ? Map.of() : parameterDocs;
        Map<String, String> defaults = parameterDefaults == null ? Map.of() : parameterDefaults;

        List<Field> fields = new ArrayList<>(parameters == null ? 0 : parameters.size());
        if (parameters != null) {
            for (String name : parameters) {
                ArrowType type = defaults.containsKey(name)
                        ? MacroDefaultsEncoder.arrowTypeForLiteral(defaults.get(name))
                        : new ArrowType.Null();
                Map<String, String> meta = new HashMap<>();
                String doc = docs.get(name);
                if (doc != null && !doc.isEmpty()) meta.put(VGI_DOC_KEY, doc);
                FieldType ft = new FieldType(true, type, null, meta.isEmpty() ? null : meta);
                fields.add(new Field(name, ft, null));
            }
        }
        return new Schema(fields);
    }

    /**
     * Build a macro {@code arguments_schema} and serialise it to IPC bytes.
     *
     * @param parameters        ordered macro parameter names
     * @param parameterDefaults parameter-name → default-value SQL literal map; may be {@code null}/empty
     * @param parameterDocs     parameter-name → description map; may be {@code null}/empty
     * @return the IPC-encoded schema bytes
     */
    public static byte[] toIpcBytes(List<String> parameters,
                                    Map<String, String> parameterDefaults,
                                    Map<String, String> parameterDocs) {
        return SchemaUtil.serializeSchema(build(parameters, parameterDefaults, parameterDocs));
    }

    /**
     * Extract per-parameter descriptions from a macro {@code arguments_schema}.
     * Inverse of {@link #build}'s {@code vgi_doc} handling: reads the
     * {@code vgi_doc} field metadata (UTF-8) per field. Undocumented fields are
     * omitted from the result.
     *
     * @param schema a macro {@code arguments_schema} (one field per parameter)
     * @return parameter-name → description, for documented parameters only
     */
    public static Map<String, String> parameterDocsFromSchema(Schema schema) {
        Map<String, String> docs = new java.util.LinkedHashMap<>();
        if (schema == null) return docs;
        for (Field f : schema.getFields()) {
            Map<String, String> meta = f.getMetadata();
            if (meta == null) continue;
            String doc = meta.get(VGI_DOC_KEY);
            if (doc != null && !doc.isEmpty()) docs.put(f.getName(), doc);
        }
        return docs;
    }
}
