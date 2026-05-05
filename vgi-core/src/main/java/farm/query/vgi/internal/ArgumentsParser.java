// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.function.Arguments;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
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
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decode {@code BindRequest.arguments} IPC bytes into a typed {@link Arguments}.
 *
 * <p>DuckDB wraps every argument in a single {@code args} struct column whose
 * fields are named {@code positional_0}, {@code positional_1}, ... for
 * positional args (in order) and {@code named_<name>} for named args. The
 * batch has at most 1 row; const-param values appear at row 0, column-param
 * placeholders are typically null.
 *
 * <p>Mirrors {@code vgi-go ParseArguments}.
 */
public final class ArgumentsParser {

    private ArgumentsParser() {}

    public static Arguments parse(byte[] data) {
        if (data == null || data.length == 0) return Arguments.empty();
        try (ByteArrayInputStream in = new ByteArrayInputStream(data);
             ArrowStreamReader reader = new ArrowStreamReader(in, Allocators.root())) {
            if (!reader.loadNextBatch()) return Arguments.empty();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (root.getRowCount() == 0) return Arguments.empty();

            // Only the "args" struct shape is in scope for Phase 2.
            if (root.getFieldVectors().size() == 1
                    && "args".equals(root.getVector(0).getName())
                    && root.getVector(0) instanceof StructVector args) {
                return extractFromStruct(args);
            }
            // Fallback: flat columns named directly by parameter name.
            return extractFlat(root);
        } catch (Exception e) {
            throw new RuntimeException("ArgumentsParser failed", e);
        }
    }

    private static Arguments extractFromStruct(StructVector args) {
        Map<Integer, Object> positionalByIdx = new LinkedHashMap<>();
        Map<String, Object> named = new LinkedHashMap<>();
        for (Field f : args.getField().getChildren()) {
            String name = f.getName();
            FieldVector child = args.getChild(name);
            Object value = readScalar(child);
            if (name.startsWith("positional_")) {
                int idx = Integer.parseInt(name.substring("positional_".length()));
                positionalByIdx.put(idx, value);
                named.put(name, value);
            } else if (name.startsWith("named_")) {
                String pretty = name.substring("named_".length());
                named.put(pretty, value);
                named.put(name, value);
            } else {
                named.put(name, value);
            }
        }
        List<Object> positional = new ArrayList<>(positionalByIdx.size());
        for (int i = 0; i < positionalByIdx.size(); i++) {
            positional.add(positionalByIdx.getOrDefault(i, null));
        }
        return new Arguments(java.util.Collections.unmodifiableList(positional),
                java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(named)));
    }

    private static Arguments extractFlat(VectorSchemaRoot root) {
        List<Object> positional = new ArrayList<>();
        Map<String, Object> named = new LinkedHashMap<>();
        for (FieldVector v : root.getFieldVectors()) {
            Object value = readScalar(v);
            named.put(v.getName(), value);
            positional.add(value);
        }
        return new Arguments(java.util.Collections.unmodifiableList(positional),
                java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(named)));
    }

    /** Read the scalar (row 0) value out of a vector. Returns null for null/empty. */
    private static Object readScalar(FieldVector v) {
        if (v.getValueCount() == 0) return null;
        return readScalarAt(v, 0);
    }

    private static Object readScalarAt(FieldVector v, int row) {
        // Struct vectors may carry valid children even when the struct itself
        // is reported as "null" — the args wrapper sometimes leaves the
        // top-level struct mask unset. Recurse into children so callers see
        // populated maps rather than a top-level null.
        if (!(v instanceof StructVector) && v.isNull(row)) return null;
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
        if (v instanceof StructVector sv) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Field f : sv.getField().getChildren()) {
                FieldVector child = sv.getChild(f.getName());
                out.put(f.getName(), readScalarAt(child, row));
            }
            return out;
        }
        // Fallback: leave as the vector itself for callers that need column-level access.
        return v;
    }
}
