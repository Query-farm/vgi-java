// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
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
 * Hand-rolled serialiser for {@link MacroInfo}. The C++ extension's
 * {@code MacroInfoSchema} expects {@code macro_type} as
 * {@code dictionary<int16, utf8>} and {@code parameter_default_values} as
 * non-nullable {@code binary} (empty bytes when absent).
 */
public final class MacroInfoSerializer {

    private MacroInfoSerializer() {}

    private static final ArrowType I16 = new ArrowType.Int(16, true);
    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();
    private static final long DICT_MACRO_TYPE = 100L;
    private static final List<String> MACRO_TYPE_VALUES = List.of("scalar", "table");

    public static byte[] serialize(MacroInfo info) {
        BufferAllocator alloc = Allocators.root();
        Schema schema = buildSchema();
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        registerDict(provider, alloc, DICT_MACRO_TYPE, MACRO_TYPE_VALUES);

        List<FieldVector> vectors = new ArrayList<>();
        Map<String, FieldVector> byName = new HashMap<>();
        for (Field f : schema.getFields()) {
            FieldVector v;
            if (f.getDictionary() != null) {
                Field indexField = new Field(f.getName(),
                        new FieldType(f.isNullable(), I16, f.getDictionary()), null);
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
            throw new RuntimeException("MacroInfo serialize failed", e);
        } finally {
            for (FieldVector v : vectors) v.close();
            for (long id : provider.getDictionaryIds()) provider.lookup(id).getVector().close();
        }
    }

    private static Schema buildSchema() {
        return new Schema(List.of(
                nullable("comment", UTF8),
                mapField("tags"),
                nonNull("name", UTF8),
                nonNull("schema_name", UTF8),
                dict("macro_type", DICT_MACRO_TYPE),
                listField("parameters", new Field("item",
                        new FieldType(true, UTF8, null), null)),
                nonNull("parameter_default_values", BINARY),
                nonNull("definition", UTF8)));
    }

    private static Field nonNull(String name, ArrowType t) {
        return new Field(name, new FieldType(false, t, null), null);
    }

    private static Field nullable(String name, ArrowType t) {
        return new Field(name, new FieldType(true, t, null), null);
    }

    private static Field dict(String name, long id) {
        DictionaryEncoding enc = new DictionaryEncoding(id, false, (ArrowType.Int) I16);
        return new Field(name, new FieldType(false, UTF8, enc), null);
    }

    private static Field mapField(String name) {
        Field key = new Field("key", new FieldType(false, UTF8, null), null);
        Field val = new Field("value", new FieldType(true, UTF8, null), null);
        Field entries = new Field("entries", new FieldType(false, new ArrowType.Struct(), null),
                List.of(key, val));
        return new Field(name, new FieldType(false, new ArrowType.Map(false), null), List.of(entries));
    }

    private static Field listField(String name, Field item) {
        return new Field(name, new FieldType(false, new ArrowType.List(), null), List.of(item));
    }

    private static void registerDict(DictionaryProvider.MapDictionaryProvider provider,
                                       BufferAllocator alloc, long id, List<String> values) {
        Field f = new Field("", new FieldType(true, UTF8, null), null);
        VarCharVector vec = (VarCharVector) f.createVector(alloc);
        vec.allocateNew();
        for (int i = 0; i < values.size(); i++) vec.setSafe(i, new Text(values.get(i)));
        vec.setValueCount(values.size());
        provider.put(new Dictionary(vec, new DictionaryEncoding(id, false, (ArrowType.Int) I16)));
    }

    private static void populate(Map<String, FieldVector> v, MacroInfo info) {
        setVarChar(v, "comment", info.comment());
        setMap(v, "tags", info.tags());
        setVarChar(v, "name", info.name());
        setVarChar(v, "schema_name", info.schema_name());
        setDictIndex(v, "macro_type", info.macro_type());
        setStringList(v, "parameters", info.parameters());
        setVarBinary(v, "parameter_default_values",
                info.parameter_default_values() == null ? new byte[0] : info.parameter_default_values());
        setVarChar(v, "definition", info.definition());
    }

    private static void setVarChar(Map<String, FieldVector> v, String f, String value) {
        VarCharVector vec = (VarCharVector) v.get(f);
        if (value == null) vec.setNull(0); else vec.setSafe(0, new Text(value));
    }

    private static void setVarBinary(Map<String, FieldVector> v, String f, byte[] value) {
        VarBinaryVector vec = (VarBinaryVector) v.get(f);
        vec.setSafe(0, value == null ? new byte[0] : value);
    }

    private static void setDictIndex(Map<String, FieldVector> v, String f, String value) {
        SmallIntVector vec = (SmallIntVector) v.get(f);
        int idx = MACRO_TYPE_VALUES.indexOf(value);
        if (idx < 0) throw new IllegalArgumentException("unknown macro_type '" + value + "'");
        vec.setSafe(0, idx);
    }

    private static void setStringList(Map<String, FieldVector> v, String f, List<String> values) {
        ListVector vec = (ListVector) v.get(f);
        UnionListWriter w = vec.getWriter();
        w.startList();
        if (values != null) for (String s : values) if (s != null) w.varChar().writeVarChar(s);
        w.endList();
        w.setValueCount(1);
    }

    private static void setMap(Map<String, FieldVector> v, String f, Map<String, String> values) {
        MapVector vec = (MapVector) v.get(f);
        UnionMapWriter w = vec.getWriter();
        w.startMap();
        if (values != null) {
            for (Map.Entry<String, String> e : values.entrySet()) {
                w.startEntry();
                w.key().varChar().writeVarChar(e.getKey());
                if (e.getValue() != null) w.value().varChar().writeVarChar(e.getValue());
                w.endEntry();
            }
        }
        w.endMap();
        w.setValueCount(1);
    }
}
