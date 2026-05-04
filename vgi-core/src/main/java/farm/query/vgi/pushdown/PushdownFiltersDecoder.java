// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.pushdown;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
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

    public static PushdownFilters decode(byte[] data) {
        return decode(data, List.of());
    }

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
        String type = spec.path("type").asText("");
        String colName = spec.path("column_name").asText("");
        int colIdx = spec.path("column_index").asInt(-1);
        return switch (type) {
            case "constant" -> {
                // value_ref N points to column N+1 (column 0 holds the JSON specs).
                int valueRef = spec.path("value_ref").asInt(-1);
                String op = spec.path("op").asText("");
                Object value = readScalarAt(root, valueRef + 1, 0);
                yield new PushdownFilter.Constant(colName, colIdx, op, value);
            }
            case "is_null" -> new PushdownFilter.IsNull(colName, colIdx);
            case "is_not_null" -> new PushdownFilter.IsNotNull(colName, colIdx);
            case "in" -> {
                int valueRef = spec.path("value_ref").asInt(-1);
                List<Object> values = readListAt(root, valueRef + 1, 0);
                yield new PushdownFilter.In(colName, colIdx, values);
            }
            case "join_keys" -> {
                // Resolve via joinKeysMap: the actual values are provided
                // out-of-band in InitRequest.join_keys.
                String keysCol = spec.path("keys_column").asText("");
                List<Object> values = joinKeys.getOrDefault(keysCol, List.of());
                yield new PushdownFilter.In(colName, colIdx, values);
            }
            case "and" -> new PushdownFilter.And(colName, colIdx, parseChildren(spec, root, joinKeys));
            case "or" -> new PushdownFilter.Or(colName, colIdx, parseChildren(spec, root, joinKeys));
            case "struct" -> {
                int childIdx = spec.path("child_index").asInt(0);
                String childName = spec.path("child_name").asText("");
                JsonNode childSpec = spec.get("child_filter");
                PushdownFilter child = childSpec == null ? null : parseSpec(childSpec, root, joinKeys);
                yield new PushdownFilter.Struct(colName, colIdx, childIdx, childName, child);
            }
            default -> null;
        };
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
        FieldVector v = root.getVector(colIdx);
        if (v.isNull(row)) return null;
        if (v instanceof BigIntVector b) return b.get(row);
        if (v instanceof IntVector i) return (long) i.get(row);
        if (v instanceof SmallIntVector s) return (long) s.get(row);
        if (v instanceof TinyIntVector t) return (long) t.get(row);
        if (v instanceof Float8Vector f) return f.get(row);
        if (v instanceof Float4Vector f) return (double) f.get(row);
        if (v instanceof BitVector b) return b.get(row) != 0;
        if (v instanceof VarCharVector vc) {
            byte[] bytes = vc.get(row);
            return bytes == null ? null : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        if (v instanceof LargeVarCharVector vc) {
            byte[] bytes = vc.get(row);
            return bytes == null ? null : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        if (v instanceof VarBinaryVector vb) return vb.get(row);
        if (v instanceof LargeVarBinaryVector vb) return vb.get(row);
        if (v instanceof DecimalVector d) return d.getObject(row);
        if (v instanceof DateDayVector d) return (long) d.get(row);
        return v.getObject(row);
    }

    private static List<Object> readListAt(VectorSchemaRoot root, int colIdx, int row) {
        if (colIdx < 0 || colIdx >= root.getFieldVectors().size()) return List.of();
        FieldVector v = root.getVector(colIdx);
        if (!(v instanceof ListVector lv) || lv.isNull(row)) return List.of();
        int start = lv.getElementStartIndex(row);
        int end = lv.getElementEndIndex(row);
        FieldVector inner = lv.getDataVector();
        // Wrap inner in a 1-col VSR so we can reuse readScalarAt.
        VectorSchemaRoot wrap = new VectorSchemaRoot(
                List.of(inner.getField()),
                List.of(inner),
                inner.getValueCount());
        List<Object> out = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) out.add(readScalarAt(wrap, 0, i));
        return out;
    }
}
