// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.ArgSpec;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encode/decode an {@link ArgSpec} list as an Arrow {@link Schema}.
 *
 * <p>Mirrors {@code vgi-python/vgi/argument_spec.py argument_specs_to_schema}:
 * <ul>
 *   <li>Each ArgSpec → one Schema {@link Field}.</li>
 *   <li>Field name = ArgSpec name.</li>
 *   <li>Field type = ArgSpec arrowType.</li>
 *   <li>Field metadata carries {@code vgi_arg=named}, {@code vgi_const=true},
 *       {@code vgi_varargs=true} as needed.</li>
 * </ul>
 */
public final class ArgumentSpecSerializer {

    private ArgumentSpecSerializer() {}

    /**
     * Encode argument specs as a {@link Schema} — one field per spec, positional
     * first (by position), then named, with VGI metadata flags applied.
     *
     * @param specs the argument specs to encode
     * @return the schema describing the function's arguments
     */
    public static Schema toSchema(List<ArgSpec> specs) {
        List<Field> fields = new ArrayList<>(specs.size());
        // Sort: positional first by position, named after.
        List<ArgSpec> sorted = new ArrayList<>(specs);
        sorted.sort((a, b) -> {
            int ka = a.position() >= 0 ? a.position() : Integer.MAX_VALUE;
            int kb = b.position() >= 0 ? b.position() : Integer.MAX_VALUE;
            return Integer.compare(ka, kb);
        });
        for (ArgSpec spec : sorted) {
            Map<String, String> meta = new HashMap<>();
            if (spec.position() < 0) meta.put("vgi_arg", "named");
            if (spec.isConst()) meta.put("vgi_const", "true");
            if (spec.varargs()) meta.put("vgi_varargs", "true");
            if (spec.anyType()) meta.put("vgi_type", "any");
            if (spec.tableInput()) meta.put("vgi_type", "table");
            // Per-argument description (UTF-8; presence-only — omit when empty).
            if (spec.doc() != null && !spec.doc().isEmpty()) meta.put("vgi_doc", spec.doc());
            FieldType ft = new FieldType(true, spec.arrowType(), null,
                    meta.isEmpty() ? null : meta);
            fields.add(new Field(spec.name(), ft, spec.children()));
        }
        return new Schema(fields);
    }

    /**
     * Encode argument specs to IPC schema bytes.
     *
     * @param specs the argument specs to encode
     * @return the IPC-encoded schema bytes
     */
    public static byte[] toIpcBytes(List<ArgSpec> specs) {
        return SchemaUtil.serializeSchema(toSchema(specs));
    }
}
