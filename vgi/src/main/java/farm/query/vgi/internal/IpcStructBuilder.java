// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
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
import java.util.function.Consumer;

/**
 * Shared helpers for building one-row IPC record batches. Used by the
 * {@code *InfoSerializer} family to declare {@link Field}s and write primitive
 * / list / map / dict-encoded values without re-implementing the same dispatch
 * each time.
 */
public final class IpcStructBuilder {

    /** Signed 16-bit integer Arrow type. */
    public static final ArrowType I16 = new ArrowType.Int(16, true);
    /** Signed 32-bit integer Arrow type. */
    public static final ArrowType I32 = new ArrowType.Int(32, true);
    /** Signed 64-bit integer Arrow type. */
    public static final ArrowType I64 = new ArrowType.Int(64, true);
    /** UTF-8 string Arrow type. */
    public static final ArrowType UTF8 = new ArrowType.Utf8();
    /** Variable-length binary Arrow type. */
    public static final ArrowType BINARY = new ArrowType.Binary();
    /** Boolean Arrow type. */
    public static final ArrowType BOOL = new ArrowType.Bool();

    private IpcStructBuilder() {}

    /* ============ field factories ============ */

    /**
     * A non-nullable scalar field.
     *
     * @param name field name
     * @param type field Arrow type
     * @return the field
     */
    public static Field nonNull(String name, ArrowType type) {
        return new Field(name, new FieldType(false, type, null), null);
    }

    /**
     * A nullable scalar field.
     *
     * @param name field name
     * @param type field Arrow type
     * @return the field
     */
    public static Field nullable(String name, ArrowType type) {
        return new Field(name, new FieldType(true, type, null), null);
    }

    /**
     * A {@code dictionary<int16, utf8>} field. The C++ extension expects
     * enum-shaped fields in this exact shape.
     *
     * @param name     field name
     * @param id       dictionary id (see {@link DictionaryIds})
     * @param nullable whether the field is nullable
     * @return the dict-encoded field
     */
    public static Field dict(String name, long id, boolean nullable) {
        DictionaryEncoding enc = new DictionaryEncoding(id, false, (ArrowType.Int) I16);
        return new Field(name, new FieldType(nullable, UTF8, enc), null);
    }

    /**
     * Non-null {@code map<utf8, utf8>} (values nullable).
     *
     * @param name field name
     * @return the map field
     */
    public static Field mapUtf8Utf8(String name) {
        Field key = new Field("key", new FieldType(false, UTF8, null), null);
        Field val = new Field("value", new FieldType(true, UTF8, null), null);
        Field entries = new Field("entries",
                new FieldType(false, new ArrowType.Struct(), null),
                List.of(key, val));
        return new Field(name,
                new FieldType(false, new ArrowType.Map(false), null),
                List.of(entries));
    }

    /**
     * Non-null {@code list<itemType>} with the given item field.
     *
     * @param name field name
     * @param item the list's item field
     * @return the list field
     */
    public static Field listOf(String name, Field item) {
        return new Field(name, new FieldType(false, new ArrowType.List(), null), List.of(item));
    }

    /**
     * Non-null {@code list<itemType>} where item is a primitive (nullable).
     *
     * @param name field name
     * @param item the item Arrow type
     * @return the list field
     */
    public static Field listOfPrim(String name, ArrowType item) {
        return listOf(name, new Field("item", new FieldType(true, item, null), null));
    }

    /**
     * Non-null {@code list<list<int32>>} (inner items nullable).
     *
     * @param name field name
     * @return the nested-list field
     */
    public static Field listOfListOfInt32(String name) {
        Field inner = new Field("item", new FieldType(true, I32, null), null);
        Field outerItem = new Field("item",
                new FieldType(true, new ArrowType.List(), null), List.of(inner));
        return listOf(name, outerItem);
    }

    /**
     * Non-null {@code list<list<utf8>>} (inner items nullable). Mirrors
     * {@link #listOfListOfInt32(String)} for the string variant — used by
     * {@code TableInfo.required_filters} (conjunctive normal form: an AND of
     * OR-groups of dotted column paths).
     *
     * @param name field name
     * @return the nested-list field
     */
    public static Field listOfListOfUtf8(String name) {
        Field inner = new Field("item", new FieldType(true, UTF8, null), null);
        Field outerItem = new Field("item",
                new FieldType(true, new ArrowType.List(), null), List.of(inner));
        return listOf(name, outerItem);
    }

    /* ============ value writers — operate on row 0 ============ */

    /**
     * Write a string (or null) into row 0 of a {@code VarCharVector}.
     *
     * @param v     the target vector
     * @param value the string, or {@code null}
     */
    public static void writeVarChar(FieldVector v, String value) {
        VarCharVector vc = (VarCharVector) v;
        if (value == null) vc.setNull(0); else vc.setSafe(0, new Text(value));
    }

    /**
     * Write binary into row 0, treating {@code null} as the empty byte array
     * (matches the wire shape of every {@code non-null binary} field the C++
     * side reads).
     *
     * @param v     the target vector
     * @param value the bytes, or {@code null} (written as empty)
     */
    public static void writeVarBinarySafe(FieldVector v, byte[] value) {
        VarBinaryVector vb = (VarBinaryVector) v;
        vb.setSafe(0, value == null ? new byte[0] : value);
    }

    /**
     * Write binary (or null) into row 0 of a {@code VarBinaryVector}.
     *
     * @param v     the target vector
     * @param value the bytes, or {@code null}
     */
    public static void writeNullableVarBinary(FieldVector v, byte[] value) {
        VarBinaryVector vb = (VarBinaryVector) v;
        if (value == null) vb.setNull(0); else vb.setSafe(0, value);
    }

    /**
     * Write a boolean into row 0 of a {@code BitVector}.
     *
     * @param v     the target vector
     * @param value the boolean value
     */
    public static void writeBool(FieldVector v, boolean value) {
        ((BitVector) v).setSafe(0, value ? 1 : 0);
    }

    /**
     * Write a boolean (or null) into row 0 of a {@code BitVector}.
     *
     * @param v     the target vector
     * @param value the boolean, or {@code null}
     */
    public static void writeNullableBool(FieldVector v, Boolean value) {
        BitVector bv = (BitVector) v;
        if (value == null) bv.setNull(0); else bv.setSafe(0, value ? 1 : 0);
    }

    /**
     * Write an int into row 0 of an {@code IntVector}.
     *
     * @param v     the target vector
     * @param value the value
     */
    public static void writeInt32(FieldVector v, int value) {
        ((IntVector) v).setSafe(0, value);
    }

    /**
     * Write a long (or null) into row 0 of a {@code BigIntVector}.
     *
     * @param v     the target vector
     * @param value the value, or {@code null}
     */
    public static void writeNullableInt64(FieldVector v, Long value) {
        BigIntVector bi = (BigIntVector) v;
        if (value == null) bi.setNull(0); else bi.setSafe(0, value);
    }

    /**
     * Dict-encoded utf8 manifests as a {@link SmallIntVector} (int16 indices).
     * Look up the value's position in the dictionary and write it.
     *
     * @param v          the int16 index vector
     * @param value      the value to encode, or {@code null}
     * @param dictValues the ordered dictionary values
     * @param fieldName  field name, for the error message
     * @throws IllegalArgumentException if {@code value} is not in {@code dictValues}
     */
    public static void writeDictIndex(FieldVector v, String value, List<String> dictValues, String fieldName) {
        SmallIntVector iv = (SmallIntVector) v;
        if (value == null) { iv.setNull(0); return; }
        int idx = dictValues.indexOf(value);
        if (idx < 0) {
            throw new IllegalArgumentException("unknown dict value '" + value + "' for " + fieldName);
        }
        iv.setSafe(0, idx);
    }

    /**
     * Write a list of strings into row 0 of a {@code ListVector}. Nulls in
     * {@code values} are skipped.
     *
     * @param v      the target list vector
     * @param values the strings, or {@code null} for an empty list
     */
    public static void writeStringList(FieldVector v, List<String> values) {
        ListVector lv = (ListVector) v;
        UnionListWriter w = lv.getWriter();
        w.startList();
        if (values != null) for (String s : values) if (s != null) w.varChar().writeVarChar(s);
        w.endList();
        w.setValueCount(1);
    }

    /**
     * Write a list of ints into row 0 of a {@code ListVector}. Nulls in
     * {@code values} are skipped.
     *
     * @param v      the target list vector
     * @param values the ints, or {@code null} for an empty list
     */
    public static void writeListInt32(FieldVector v, List<Integer> values) {
        ListVector lv = (ListVector) v;
        UnionListWriter w = lv.getWriter();
        w.startList();
        if (values != null) for (Integer i : values) if (i != null) w.integer().writeInt(i);
        w.endList();
        w.setValueCount(1);
    }

    /**
     * Write a list of int lists into row 0 of a nested {@code ListVector}.
     * Nulls are skipped.
     *
     * @param v      the target nested list vector
     * @param values the inner lists, or {@code null} for an empty list
     */
    public static void writeListListInt32(FieldVector v, List<List<Integer>> values) {
        ListVector lv = (ListVector) v;
        UnionListWriter w = lv.getWriter();
        w.startList();
        if (values != null) {
            for (List<Integer> inner : values) {
                w.list().startList();
                if (inner != null) for (Integer i : inner) if (i != null) w.list().integer().writeInt(i);
                w.list().endList();
            }
        }
        w.endList();
        w.setValueCount(1);
    }

    /**
     * Write a list of string lists into row 0 of a nested {@code ListVector}.
     * Nulls are skipped. Mirrors {@link #writeListListInt32} for the string
     * variant (e.g. {@code TableInfo.required_filters}).
     *
     * @param v      the target nested list vector
     * @param values the inner lists, or {@code null} for an empty list
     */
    public static void writeListListString(FieldVector v, List<List<String>> values) {
        ListVector lv = (ListVector) v;
        UnionListWriter w = lv.getWriter();
        w.startList();
        if (values != null) {
            for (List<String> inner : values) {
                w.list().startList();
                if (inner != null) for (String s : inner) if (s != null) w.list().varChar().writeVarChar(s);
                w.list().endList();
            }
        }
        w.endList();
        w.setValueCount(1);
    }

    /**
     * Write a list of binary blobs into row 0 of a {@code ListVector}. Nulls in
     * {@code values} are skipped.
     *
     * @param v      the target list vector
     * @param values the blobs, or {@code null} for an empty list
     */
    public static void writeListBinary(FieldVector v, List<byte[]> values) {
        ListVector lv = (ListVector) v;
        UnionListWriter w = lv.getWriter();
        w.startList();
        if (values != null) {
            BufferAllocator alloc = Allocators.root();
            for (byte[] b : values) {
                if (b == null) continue;
                try (ArrowBuf buf = alloc.buffer(b.length)) {
                    buf.writeBytes(b);
                    w.varBinary().writeVarBinary(0, b.length, buf);
                }
            }
        }
        w.endList();
        w.setValueCount(1);
    }

    /**
     * Write a string→string map into row 0 of a {@code MapVector}. Null values
     * are written as null entries.
     *
     * @param v      the target map vector
     * @param values the entries, or {@code null} for an empty map
     */
    public static void writeMap(FieldVector v, Map<String, String> values) {
        MapVector mv = (MapVector) v;
        UnionMapWriter w = mv.getWriter();
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

    /* ============ dictionary registration ============ */

    /**
     * Register a {@code dictionary<int16, utf8>} dictionary into {@code provider}.
     *
     * @param provider the provider to populate
     * @param alloc    allocator for the dictionary vector
     * @param id       the dictionary id
     * @param values   the ordered dictionary values
     */
    public static void registerDict(DictionaryProvider.MapDictionaryProvider provider,
                                     BufferAllocator alloc, long id, List<String> values) {
        Field f = new Field("", new FieldType(true, UTF8, null), null);
        VarCharVector vec = (VarCharVector) f.createVector(alloc);
        vec.allocateNew();
        for (int i = 0; i < values.size(); i++) vec.setSafe(i, new Text(values.get(i)));
        vec.setValueCount(values.size());
        provider.put(new Dictionary(vec, new DictionaryEncoding(id, false, (ArrowType.Int) I16)));
    }

    /* ============ workflow ============ */

    /**
     * Allocates vectors for {@code schema} (instantiating the int16 index
     * vector for dict-encoded fields), calls {@code populator} with a
     * name → vector map keyed at row 0, writes a 1-row IPC stream, and
     * closes all vectors plus dictionary entries afterwards.
     *
     * @param schema    the one-row schema to build
     * @param provider  dictionary provider for any dict-encoded fields
     * @param populator callback that writes row-0 values, keyed by field name
     * @return the IPC stream bytes
     */
    public static byte[] build(Schema schema,
                                 DictionaryProvider.MapDictionaryProvider provider,
                                 Consumer<Map<String, FieldVector>> populator) {
        BufferAllocator alloc = Allocators.root();
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
            populator.accept(byName);
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
            throw new RuntimeException("IpcStructBuilder.build failed", e);
        } finally {
            for (FieldVector v : vectors) v.close();
            if (provider != null) {
                for (long id : provider.getDictionaryIds()) provider.lookup(id).getVector().close();
            }
        }
    }

    /**
     * Convenience for non-dict-encoded schemas.
     *
     * @param schema    the one-row schema to build
     * @param populator callback that writes row-0 values, keyed by field name
     * @return the IPC stream bytes
     */
    public static byte[] build(Schema schema, Consumer<Map<String, FieldVector>> populator) {
        return build(schema, new DictionaryProvider.MapDictionaryProvider(), populator);
    }
}
