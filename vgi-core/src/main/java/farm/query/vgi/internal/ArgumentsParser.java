// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.function.Arguments;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
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
        Map<Integer, org.apache.arrow.vector.types.pojo.ArrowType> positionalTypeByIdx = new LinkedHashMap<>();
        Map<String, Object> named = new LinkedHashMap<>();
        for (Field f : args.getField().getChildren()) {
            String name = f.getName();
            FieldVector child = args.getChild(name);
            Object value = readScalar(child);
            if (name.startsWith("positional_")) {
                int idx = Integer.parseInt(name.substring("positional_".length()));
                positionalByIdx.put(idx, value);
                positionalTypeByIdx.put(idx, f.getType());
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
        List<org.apache.arrow.vector.types.pojo.ArrowType> positionalTypes = new ArrayList<>(positionalTypeByIdx.size());
        for (int i = 0; i < positionalByIdx.size(); i++) {
            positional.add(positionalByIdx.getOrDefault(i, null));
            positionalTypes.add(positionalTypeByIdx.getOrDefault(i, null));
        }
        return new Arguments(java.util.Collections.unmodifiableList(positional),
                java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(named)),
                java.util.Collections.unmodifiableList(positionalTypes));
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
        return VectorScalarCodec.read(v, 0);
    }
}
