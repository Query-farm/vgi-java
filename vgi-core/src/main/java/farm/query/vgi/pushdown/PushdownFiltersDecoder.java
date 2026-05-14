// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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
