// Copyright 2025-2026 Query.Farm LLC

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

            // ArrowStreamReader IS a DictionaryProvider; pass it through so
            // dict-encoded args (DuckDB ENUMs) resolve to their underlying
            // value (e.g. 'happy') rather than the raw index byte.
            org.apache.arrow.vector.dictionary.DictionaryProvider provider = reader;
            // Only the "args" struct shape is in scope for Phase 2.
            if (root.getFieldVectors().size() == 1
                    && "args".equals(root.getVector(0).getName())
                    && root.getVector(0) instanceof StructVector args) {
                return extractFromStruct(args, provider);
            }
            // Fallback: flat columns named directly by parameter name.
            return extractFlat(root, provider);
        } catch (Exception e) {
            throw new RuntimeException("ArgumentsParser failed", e);
        }
    }

    private static Arguments extractFromStruct(StructVector args,
                                                  org.apache.arrow.vector.dictionary.DictionaryProvider provider) {
        Map<Integer, Object> positionalByIdx = new LinkedHashMap<>();
        Map<Integer, org.apache.arrow.vector.types.pojo.ArrowType> positionalTypeByIdx = new LinkedHashMap<>();
        Map<Integer, Field> positionalFieldByIdx = new LinkedHashMap<>();
        Map<String, Object> named = new LinkedHashMap<>();
        for (Field f : args.getField().getChildren()) {
            String name = f.getName();
            FieldVector child = args.getChild(name);
            Object value = readScalarResolvingDict(child, f, provider);
            if (name.startsWith("positional_")) {
                int idx = Integer.parseInt(name.substring("positional_".length()));
                positionalByIdx.put(idx, value);
                positionalTypeByIdx.put(idx, f.getType());
                positionalFieldByIdx.put(idx, f);
                named.put(name, value);
            } else if (name.startsWith("named_")) {
                String pretty = name.substring("named_".length());
                named.put(pretty, value);
                named.put(name, value);
            } else {
                named.put(name, value);
            }
        }
        int n = positionalByIdx.size();
        List<Object> positional = new ArrayList<>(n);
        List<org.apache.arrow.vector.types.pojo.ArrowType> positionalTypes = new ArrayList<>(n);
        List<Field> positionalFields = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            positional.add(positionalByIdx.getOrDefault(i, null));
            positionalTypes.add(positionalTypeByIdx.getOrDefault(i, null));
            positionalFields.add(positionalFieldByIdx.getOrDefault(i, null));
        }
        return new Arguments(java.util.Collections.unmodifiableList(positional),
                java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(named)),
                java.util.Collections.unmodifiableList(positionalTypes),
                java.util.Collections.unmodifiableList(positionalFields));
    }

    private static Arguments extractFlat(VectorSchemaRoot root,
                                            org.apache.arrow.vector.dictionary.DictionaryProvider provider) {
        List<Object> positional = new ArrayList<>();
        Map<String, Object> named = new LinkedHashMap<>();
        for (FieldVector v : root.getFieldVectors()) {
            Object value = readScalarResolvingDict(v, v.getField(), provider);
            named.put(v.getName(), value);
            positional.add(value);
        }
        return new Arguments(java.util.Collections.unmodifiableList(positional),
                java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(named)));
    }

    /**
     * Read the scalar (row 0) value out of a vector. For dict-encoded fields
     * (DuckDB ENUMs), look up the underlying dictionary value via {@code
     * provider} and return that — not the raw index byte. {@code null} for
     * null/empty.
     */
    private static Object readScalarResolvingDict(FieldVector v, Field f,
                                                    org.apache.arrow.vector.dictionary.DictionaryProvider provider) {
        if (v.getValueCount() == 0) return null;
        org.apache.arrow.vector.types.pojo.DictionaryEncoding enc = f.getDictionary();
        if (enc != null && provider != null) {
            org.apache.arrow.vector.dictionary.Dictionary d = provider.lookup(enc.getId());
            if (d != null && !v.isNull(0)) {
                int idx;
                Object raw = VectorScalarCodec.read(v, 0);
                if (raw instanceof Number n) idx = n.intValue();
                else if (raw instanceof Character c) idx = c.charValue();
                else return raw;  // can't resolve; pass through
                org.apache.arrow.vector.FieldVector dv = d.getVector();
                if (idx >= 0 && idx < dv.getValueCount()) {
                    return VectorScalarCodec.read(dv, idx);
                }
            }
        }
        return VectorScalarCodec.read(v, 0);
    }
}
