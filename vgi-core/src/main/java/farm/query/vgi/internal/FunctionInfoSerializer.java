// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hand-rolled serializer for {@link FunctionInfo}. The C++ extension's
 * {@code FunctionInfoSchema} requires {@code dictionary<int16, utf8>} for the
 * enum-shaped fields ({@code function_type}, {@code stability},
 * {@code null_handling}, etc.). vgi-rpc-java's stock {@code RecordCodec} writes
 * those as plain utf8, so we build the IPC batch manually.
 */
final class FunctionInfoSerializer {

    private FunctionInfoSerializer() {}

    private static final ArrowType I16 = new ArrowType.Int(16, true);
    private static final ArrowType I32 = new ArrowType.Int(32, true);
    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();
    private static final ArrowType BOOL = new ArrowType.Bool();

    private static final long DICT_FUNCTION_TYPE = 1;
    private static final long DICT_STABILITY = 2;
    private static final long DICT_NULL_HANDLING = 3;
    private static final long DICT_ORDER_PRESERVATION = 4;
    private static final long DICT_ORDER_DEPENDENT = 5;
    private static final long DICT_DISTINCT_DEPENDENT = 6;

    private static final List<String> FUNCTION_TYPE_VALUES = List.of("scalar", "table", "aggregate");
    private static final List<String> STABILITY_VALUES = List.of("CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY");
    private static final List<String> NULL_HANDLING_VALUES = List.of("DEFAULT", "SPECIAL");
    private static final List<String> ORDER_PRESERVATION_VALUES =
            List.of("NO_ORDER_PRESERVED", "INSERTION_ORDER", "FIXED_ORDER");
    private static final List<String> ORDER_DEPENDENT_VALUES = List.of("NOT_ORDER_DEPENDENT", "ORDER_DEPENDENT");
    private static final List<String> DISTINCT_DEPENDENT_VALUES =
            List.of("NOT_DISTINCT_DEPENDENT", "DISTINCT_DEPENDENT");

    static byte[] serialize(FunctionInfo info) {
        BufferAllocator alloc = Allocators.root();
        Schema schema = buildSchema();
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        registerDict(provider, alloc, DICT_FUNCTION_TYPE, FUNCTION_TYPE_VALUES);
        registerDict(provider, alloc, DICT_STABILITY, STABILITY_VALUES);
        registerDict(provider, alloc, DICT_NULL_HANDLING, NULL_HANDLING_VALUES);
        registerDict(provider, alloc, DICT_ORDER_PRESERVATION, ORDER_PRESERVATION_VALUES);
        registerDict(provider, alloc, DICT_ORDER_DEPENDENT, ORDER_DEPENDENT_VALUES);
        registerDict(provider, alloc, DICT_DISTINCT_DEPENDENT, DISTINCT_DEPENDENT_VALUES);

        // Build vectors manually so dict-encoded fields end up as SmallIntVector
        // (the index vector), not VarCharVector (the value vector).
        // {@link Field#createVector} ignores the dictionary encoding for vector
        // selection — it always returns the value-type vector. We special-case
        // dict-encoded fields and instantiate the index vector ourselves.
        List<FieldVector> vectors = new ArrayList<>();
        Map<String, FieldVector> byName = new HashMap<>();
        for (Field f : schema.getFields()) {
            FieldVector v;
            if (f.getDictionary() != null) {
                Field indexField = new Field(f.getName(),
                        new FieldType(f.isNullable(), I16, f.getDictionary()),
                        null);
                v = indexField.createVector(alloc);
            } else {
                v = f.createVector(alloc);
            }
            v.allocateNew();
            vectors.add(v);
            byName.put(f.getName(), v);
        }

        try {
            populate(byName, info);
            for (FieldVector v : vectors) v.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(schema, vectors, 1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, provider, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("FunctionInfo serialize failed", e);
        } finally {
            for (FieldVector v : vectors) v.close();
            for (long id : provider.getDictionaryIds()) {
                provider.lookup(id).getVector().close();
            }
        }
    }

    private static Schema buildSchema() {
        return new Schema(List.of(
                nullable("comment", UTF8),
                mapField("tags"),
                nonNull("name", UTF8),
                nonNull("schema_name", UTF8),
                dict("function_type", DICT_FUNCTION_TYPE, false),
                nonNull("arguments", BINARY),
                nonNull("output_schema", BINARY),
                dict("stability", DICT_STABILITY, true),
                dict("null_handling", DICT_NULL_HANDLING, true),
                nonNull("description", UTF8),
                listField("examples", buildExampleStruct()),
                listField("categories", new Field("item",
                        new FieldType(true, UTF8, null), null)),
                nullable("projection_pushdown", BOOL),
                nullable("filter_pushdown", BOOL),
                nullable("sampling_pushdown", BOOL),
                listField("supported_expression_filters", new Field("item",
                        new FieldType(true, UTF8, null), null)),
                dict("order_preservation", DICT_ORDER_PRESERVATION, true),
                nonNull("max_workers", I32),
                dict("order_dependent", DICT_ORDER_DEPENDENT, false),
                dict("distinct_dependent", DICT_DISTINCT_DEPENDENT, false),
                nonNull("supports_window", BOOL),
                nonNull("streaming_partitioned", BOOL),
                nonNull("has_finalize", BOOL),
                listField("required_settings", new Field("item",
                        new FieldType(true, UTF8, null), null)),
                listField("required_secrets", buildRequiredSecretStruct())));
    }

    private static Field buildExampleStruct() {
        return new Field("item",
                new FieldType(true, new ArrowType.Struct(), null),
                List.of(nonNull("sql", UTF8),
                        nonNull("description", UTF8),
                        nullable("expected_output", UTF8)));
    }

    private static Field buildRequiredSecretStruct() {
        return new Field("item",
                new FieldType(true, new ArrowType.Struct(), null),
                List.of(nonNull("secret_type", UTF8),
                        nullable("scope", UTF8),
                        nullable("secret_name", UTF8)));
    }

    private static Field nonNull(String name, ArrowType t) {
        return new Field(name, new FieldType(false, t, null), null);
    }

    private static Field nullable(String name, ArrowType t) {
        return new Field(name, new FieldType(true, t, null), null);
    }

    private static Field dict(String name, long id, boolean nullable) {
        DictionaryEncoding enc = new DictionaryEncoding(id, false, (ArrowType.Int) I16);
        return new Field(name, new FieldType(nullable, UTF8, enc), null);
    }

    private static Field mapField(String name) {
        Field key = new Field("key", new FieldType(false, UTF8, null), null);
        Field val = new Field("value", new FieldType(true, UTF8, null), null);
        Field entries = new Field("entries",
                new FieldType(false, new ArrowType.Struct(), null),
                List.of(key, val));
        return new Field(name,
                new FieldType(false, new ArrowType.Map(false), null),
                List.of(entries));
    }

    private static Field listField(String name, Field item) {
        return new Field(name, new FieldType(false, new ArrowType.List(), null), List.of(item));
    }

    private static void registerDict(DictionaryProvider.MapDictionaryProvider provider,
                                      BufferAllocator alloc, long id, List<String> values) {
        Field f = new Field("", new FieldType(true, UTF8, null), null);
        VarCharVector vec = (VarCharVector) f.createVector(alloc);
        vec.allocateNew();
        for (int i = 0; i < values.size(); i++) {
            vec.setSafe(i, new Text(values.get(i)));
        }
        vec.setValueCount(values.size());
        DictionaryEncoding enc = new DictionaryEncoding(id, false, (ArrowType.Int) I16);
        provider.put(new Dictionary(vec, enc));
    }

    private static void populate(Map<String, FieldVector> v, FunctionInfo info) {
        setVarChar(v, "comment", info.comment());
        setMap(v, "tags", info.tags());
        setVarChar(v, "name", info.name());
        setVarChar(v, "schema_name", info.schema_name());
        setDictIndex(v, "function_type", info.function_type(), FUNCTION_TYPE_VALUES);
        setVarBinary(v, "arguments", info.arguments());
        setVarBinary(v, "output_schema", info.output_schema());
        setDictIndex(v, "stability", info.stability(), STABILITY_VALUES);
        setDictIndex(v, "null_handling", info.null_handling(), NULL_HANDLING_VALUES);
        setVarChar(v, "description", info.description());
        setStringList(v, "examples", List.of());
        setStringList(v, "categories", info.categories());
        setBool(v, "projection_pushdown", info.projection_pushdown());
        setBool(v, "filter_pushdown", info.filter_pushdown());
        setBool(v, "sampling_pushdown", info.sampling_pushdown());
        setStringList(v, "supported_expression_filters", info.supported_expression_filters());
        setDictIndex(v, "order_preservation", info.order_preservation(), ORDER_PRESERVATION_VALUES);
        setInt32(v, "max_workers", info.max_workers());
        setDictIndex(v, "order_dependent", info.order_dependent(), ORDER_DEPENDENT_VALUES);
        setDictIndex(v, "distinct_dependent", info.distinct_dependent(), DISTINCT_DEPENDENT_VALUES);
        setBool(v, "supports_window", info.supports_window());
        setBool(v, "streaming_partitioned", info.streaming_partitioned());
        setBool(v, "has_finalize", info.has_finalize());
        setStringList(v, "required_settings", info.required_settings());
        setStringList(v, "required_secrets", List.of());
    }

    private static void setVarChar(Map<String, FieldVector> v, String field, String value) {
        VarCharVector vec = (VarCharVector) v.get(field);
        if (value == null) vec.setNull(0);
        else vec.setSafe(0, new Text(value));
    }

    private static void setVarBinary(Map<String, FieldVector> v, String field, byte[] value) {
        VarBinaryVector vec = (VarBinaryVector) v.get(field);
        if (value == null) vec.setNull(0);
        else vec.setSafe(0, value);
    }

    private static void setBool(Map<String, FieldVector> v, String field, Boolean value) {
        BitVector vec = (BitVector) v.get(field);
        if (value == null) vec.setNull(0);
        else vec.setSafe(0, value ? 1 : 0);
    }

    private static void setInt32(Map<String, FieldVector> v, String field, int value) {
        IntVector vec = (IntVector) v.get(field);
        vec.setSafe(0, value);
    }

    /**
     * Dict-encoded utf8 fields manifest as a {@link SmallIntVector} (int16
     * indices) at IPC write time. Look up the value's index in the dictionary
     * and write it directly.
     */
    private static void setDictIndex(Map<String, FieldVector> v, String field,
                                      String value, List<String> dict) {
        SmallIntVector vec = (SmallIntVector) v.get(field);
        if (value == null) {
            vec.setNull(0);
            return;
        }
        int idx = dict.indexOf(value);
        if (idx < 0) throw new IllegalArgumentException("unknown dict value '" + value + "' for " + field);
        vec.setSafe(0, idx);
    }

    private static void setStringList(Map<String, FieldVector> v, String field, List<String> values) {
        ListVector vec = (ListVector) v.get(field);
        UnionListWriter writer = vec.getWriter();
        writer.startList();
        if (values != null) {
            for (String s : values) {
                if (s != null) writer.varChar().writeVarChar(s);
            }
        }
        writer.endList();
        writer.setValueCount(1);
    }

    private static void setMap(Map<String, FieldVector> v, String field, Map<String, String> values) {
        MapVector vec = (MapVector) v.get(field);
        UnionMapWriter writer = vec.getWriter();
        writer.startMap();
        if (values != null) {
            for (Map.Entry<String, String> e : values.entrySet()) {
                writer.startEntry();
                writer.key().varChar().writeVarChar(e.getKey());
                if (e.getValue() != null) writer.value().varChar().writeVarChar(e.getValue());
                writer.endEntry();
            }
        }
        writer.endMap();
        writer.setValueCount(1);
    }
}
