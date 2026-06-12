// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Decode pushdown-filter IPC bytes into a {@link PushdownFilters} AST.
 *
 * <p>Wire shape: a single Arrow record batch with column 0 holding a single
 * UTF-8 string row containing the JSON-encoded filter spec list. Siblings
 * carry the actual constant values referenced by {@code value_ref} indices
 * (so the JSON stays type-agnostic).
 */
public final class PushdownFiltersDecoder {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String SUPPORTED_VERSION = "1";

    private PushdownFiltersDecoder() {}

    /**
     * Decode pushdown-filter IPC bytes with no join-key inputs.
     *
     * @param data the pushdown-filter IPC bytes, or {@code null}/empty for none
     * @return the parsed AST, or {@link PushdownFilters#empty()} when there is nothing to decode
     * @throws RuntimeException if the version is unsupported or the bytes are malformed
     */
    public static PushdownFilters decode(byte[] data) {
        return decode(data, List.of());
    }

    /**
     * Decode pushdown-filter IPC bytes, resolving {@code join_keys} filters against
     * the supplied out-of-band key batches.
     *
     * @param data        the pushdown-filter IPC bytes, or {@code null}/empty for none
     * @param joinKeysIpc the {@code InitRequest.join_keys} IPC batches; each column becomes an
     *                    {@code IN} value set keyed by column name
     * @return the parsed AST, or {@link PushdownFilters#empty()} when there is nothing to decode
     * @throws RuntimeException if the version is unsupported or the bytes are malformed
     */
    public static PushdownFilters decode(byte[] data, List<byte[]> joinKeysIpc) {
        if (data == null || data.length == 0) return PushdownFilters.empty();
        try (ByteArrayInputStream in = new ByteArrayInputStream(data);
             ArrowStreamReader r = new ArrowStreamReader(in, Allocators.root())) {
            if (!r.loadNextBatch()) return PushdownFilters.empty();
            VectorSchemaRoot root = r.getVectorSchemaRoot();
            if (root.getSchema().getFields().isEmpty()) return PushdownFilters.empty();

            String version = "";
            Field f0 = root.getSchema().getFields().get(0);
            if (f0.getMetadata() != null) {
                String v = f0.getMetadata().get("vgi_filter_version");
                if (v != null) version = v;
            }
            if (!SUPPORTED_VERSION.equals(version)) {
                throw new RuntimeException("unsupported filter version: '" + version + "'");
            }

            FieldVector v0 = root.getVector(0);
            if (!(v0 instanceof VarCharVector specCol)) {
                throw new RuntimeException("filter column 0 is " + v0.getClass().getSimpleName()
                        + ", expected VarCharVector");
            }
            byte[] specBytes = specCol.get(0);
            if (specBytes == null) return PushdownFilters.empty();
            JsonNode specs = JSON.readTree(specBytes);
            if (!specs.isArray()) return PushdownFilters.empty();

            // Pre-decode join_keys into a name → list-of-values map.
            java.util.Map<String, List<Object>> joinKeysMap = new java.util.HashMap<>();
            for (byte[] jkIpc : joinKeysIpc) {
                if (jkIpc == null || jkIpc.length == 0) continue;
                try (ByteArrayInputStream jkIn = new ByteArrayInputStream(jkIpc);
                     ArrowStreamReader jkR = new ArrowStreamReader(jkIn, Allocators.root())) {
                    if (!jkR.loadNextBatch()) continue;
                    VectorSchemaRoot jkRoot = jkR.getVectorSchemaRoot();
                    for (int c = 0; c < jkRoot.getFieldVectors().size(); c++) {
                        FieldVector v = jkRoot.getVector(c);
                        List<Object> vals = new ArrayList<>(v.getValueCount());
                        for (int i = 0; i < v.getValueCount(); i++) vals.add(readScalarAt(jkRoot, c, i));
                        joinKeysMap.put(v.getField().getName(), vals);
                    }
                }
            }

            List<PushdownFilter> filters = new ArrayList<>();
            for (JsonNode spec : specs) {
                PushdownFilter f = parseSpec(spec, root, joinKeysMap);
                if (f != null) filters.add(f);
            }
            return new PushdownFilters(filters, version);
        } catch (Exception e) {
            throw new RuntimeException("PushdownFiltersDecoder.decode failed", e);
        }
    }

    private static PushdownFilter parseSpec(JsonNode spec, VectorSchemaRoot root,
                                              java.util.Map<String, List<Object>> joinKeys) {
        PushdownFilterType type = PushdownFilterType.fromWire(spec.path("type").asText(""));
        if (type == null) return null;
        String colName = spec.path("column_name").asText("");
        int colIdx = spec.path("column_index").asInt(-1);
        return switch (type) {
            case CONSTANT -> {
                // value_ref N points to column N+1 (column 0 holds the JSON specs).
                int valueRef = spec.path("value_ref").asInt(-1);
                ComparisonOperator op = ComparisonOperator.fromWire(spec.path("op").asText(""));
                Object value = readScalarAt(root, valueRef + 1, 0);
                yield new PushdownFilter.Constant(colName, colIdx, op, value);
            }
            case IS_NULL -> new PushdownFilter.IsNull(colName, colIdx);
            case IS_NOT_NULL -> new PushdownFilter.IsNotNull(colName, colIdx);
            case IN -> {
                int valueRef = spec.path("value_ref").asInt(-1);
                List<Object> values = readListAt(root, valueRef + 1, 0);
                yield new PushdownFilter.In(colName, colIdx, values);
            }
            case JOIN_KEYS -> {
                // Resolve via joinKeysMap: the actual values are provided
                // out-of-band in InitRequest.join_keys.
                String keysCol = spec.path("keys_column").asText("");
                List<Object> values = joinKeys.getOrDefault(keysCol, List.of());
                yield new PushdownFilter.In(colName, colIdx, values);
            }
            case AND -> new PushdownFilter.And(colName, colIdx, parseChildren(spec, root, joinKeys));
            case OR -> new PushdownFilter.Or(colName, colIdx, parseChildren(spec, root, joinKeys));
            case STRUCT -> {
                int childIdx = spec.path("child_index").asInt(0);
                String childName = spec.path("child_name").asText("");
                JsonNode childSpec = spec.get("child_filter");
                PushdownFilter child = childSpec == null ? null : parseSpec(childSpec, root, joinKeys);
                yield new PushdownFilter.Struct(colName, colIdx, childIdx, childName, child);
            }
            case EXPRESSION -> {
                JsonNode expr = spec.get("expr");
                String sql = expr == null ? "TRUE" : renderExpr(expr, colName, root);
                yield new PushdownFilter.Expression(colName, colIdx, sql);
            }
        };
    }

    /**
     * Render a pushed expression-tree node to a SQL boolean predicate over the
     * output columns. Mirrors vgi-python's {@code ExpressionNode.to_sql}: column
     * refs become the (quoted) filter column, constants are resolved from the
     * sibling value columns ({@code value_ref}), operators like {@code &&} render
     * infix, and geoarrow.wkb constants are wrapped in {@code ST_GeomFromHEXWKB}.
     */
    private static String renderExpr(JsonNode node, String columnName, VectorSchemaRoot root) {
        String exprType = node.path("expr_type").asText("");
        switch (exprType) {
            case "column_ref":
                return '"' + columnName.replace("\"", "\"\"") + '"';
            case "constant":
                return renderConstant(node.path("value_ref").asInt(-1), root);
            case "function": {
                String fn = node.path("function_name").asText("");
                List<String> args = new ArrayList<>();
                JsonNode children = node.get("children");
                if (children != null && children.isArray()) {
                    for (JsonNode c : children) args.add(renderExpr(c, columnName, root));
                }
                if (isOperatorName(fn) && args.size() == 2) {
                    return "(" + args.get(0) + " " + fn + " " + args.get(1) + ")";
                }
                return fn + "(" + String.join(", ", args) + ")";
            }
            case "comparison": {
                String opTok = node.path("op").asText("");
                ComparisonOperator op = ComparisonOperator.fromWire(opTok);
                String sym = op == null ? opTok : op.symbol();
                String left = renderExpr(node.get("left"), columnName, root);
                String right = renderExpr(node.get("right"), columnName, root);
                return "(" + left + " " + sym + " " + right + ")";
            }
            case "conjunction": {
                String joiner = "or".equals(node.path("conjunction_type").asText("")) ? " OR " : " AND ";
                List<String> parts = new ArrayList<>();
                JsonNode children = node.get("children");
                if (children != null && children.isArray()) {
                    for (JsonNode c : children) parts.add(renderExpr(c, columnName, root));
                }
                return "(" + String.join(joiner, parts) + ")";
            }
            default:
                return "TRUE";
        }
    }

    /** A function name that is all non-alphanumeric/underscore chars renders infix (e.g. {@code &&}). */
    private static boolean isOperatorName(String name) {
        if (name.isEmpty()) return false;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') return false;
        }
        return true;
    }

    /** Render the constant referenced by {@code value_ref} (sibling column {@code value_ref + 1}) as a SQL literal. */
    private static String renderConstant(int valueRef, VectorSchemaRoot root) {
        int colIdx = valueRef + 1;
        if (colIdx < 0 || colIdx >= root.getFieldVectors().size()) return "NULL";
        FieldVector v = root.getVector(colIdx);
        Object value = VectorScalarCodec.read(v, 0);
        if (value == null) return "NULL";
        if (value instanceof Boolean b) return b ? "TRUE" : "FALSE";
        if (value instanceof byte[] bytes) {
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte x : bytes) hex.append(Character.forDigit((x >> 4) & 0xF, 16))
                    .append(Character.forDigit(x & 0xF, 16));
            String extName = "";
            if (v.getField().getMetadata() != null) {
                String e = v.getField().getMetadata().get("ARROW:extension:name");
                if (e != null) extName = e;
            }
            if ("geoarrow.wkb".equals(extName)) return "ST_GeomFromHEXWKB('" + hex + "')";
            return "'\\x" + hex + "'::BLOB";
        }
        if (value instanceof Number) return value.toString();
        return "'" + value.toString().replace("'", "''") + "'";
    }

    private static List<PushdownFilter> parseChildren(JsonNode spec, VectorSchemaRoot root,
                                                        java.util.Map<String, List<Object>> joinKeys) {
        List<PushdownFilter> out = new ArrayList<>();
        JsonNode children = spec.get("children");
        if (children == null || !children.isArray()) return out;
        for (JsonNode c : children) {
            PushdownFilter f = parseSpec(c, root, joinKeys);
            if (f != null) out.add(f);
        }
        return out;
    }

    private static Object readScalarAt(VectorSchemaRoot root, int colIdx, int row) {
        if (colIdx < 0 || colIdx >= root.getFieldVectors().size()) return null;
        return VectorScalarCodec.read(root.getVector(colIdx), row);
    }

    private static List<Object> readListAt(VectorSchemaRoot root, int colIdx, int row) {
        if (colIdx < 0 || colIdx >= root.getFieldVectors().size()) return List.of();
        FieldVector v = root.getVector(colIdx);
        if (!(v instanceof ListVector lv) || lv.isNull(row)) return List.of();
        Object result = VectorScalarCodec.read(lv, row);
        @SuppressWarnings("unchecked")
        List<Object> out = result == null ? List.of() : (List<Object>) result;
        return out;
    }
}
