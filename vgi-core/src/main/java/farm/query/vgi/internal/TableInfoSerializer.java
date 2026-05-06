// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.protocol.TableInfo;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
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
 * Hand-rolled serializer for {@link TableInfo}. Matches the C++ extension's
 * {@code TableInfoSchema} field-by-field — list&lt;list&lt;int32&gt;&gt; for the
 * unique/primary-key constraints, list&lt;binary&gt; for foreign-key
 * constraints, optional binary for the four "inline function" payloads plus
 * {@code column_statistics} and {@code bind_result}, and optional int64 for
 * the inlined cardinality fields.
 */
public final class TableInfoSerializer {

    private TableInfoSerializer() {}

    private static final ArrowType I32 = new ArrowType.Int(32, true);
    private static final ArrowType I64 = new ArrowType.Int(64, true);
    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();
    private static final ArrowType BOOL = new ArrowType.Bool();

    public static byte[] serialize(TableInfo info) {
        BufferAllocator alloc = Allocators.root();
        Schema schema = buildSchema();
        List<FieldVector> vectors = new ArrayList<>();
        Map<String, FieldVector> byName = new HashMap<>();
        for (Field f : schema.getFields()) {
            FieldVector v = f.createVector(alloc);
            v.allocateNew();
            vectors.add(v);
            byName.put(f.getName(), v);
        }
        try {
            populate(byName, info);
            for (FieldVector v : vectors) v.setValueCount(1);
            VectorSchemaRoot root = new VectorSchemaRoot(schema, vectors, 1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("TableInfo serialize failed", e);
        } finally {
            for (FieldVector v : vectors) v.close();
        }
    }

    private static Schema buildSchema() {
        return new Schema(List.of(
                nullable("comment", UTF8),
                mapField("tags"),
                nonNull("name", UTF8),
                nonNull("schema_name", UTF8),
                nonNull("columns", BINARY),
                listOfInt32("not_null_constraints"),
                listOfListOfInt32("unique_constraints"),
                listOfNullableUtf8("check_constraints"),
                listOfListOfInt32("primary_key_constraints"),
                listOfBinary("foreign_key_constraints"),
                nonNull("supports_insert", BOOL),
                nonNull("supports_update", BOOL),
                nonNull("supports_delete", BOOL),
                nonNull("supports_returning", BOOL),
                nonNull("supports_column_statistics", BOOL),
                nonNull("scan_function", BINARY),
                nonNull("insert_function", BINARY),
                nonNull("update_function", BINARY),
                nonNull("delete_function", BINARY),
                nonNull("cardinality_estimate", I64),
                nonNull("cardinality_max", I64),
                nonNull("column_statistics", BINARY),
                nonNull("bind_result", BINARY)));
    }

    private static Field nonNull(String name, ArrowType t) {
        return new Field(name, new FieldType(false, t, null), null);
    }

    private static Field nullable(String name, ArrowType t) {
        return new Field(name, new FieldType(true, t, null), null);
    }

    private static Field listOfInt32(String name) {
        Field item = new Field("item", new FieldType(true, I32, null), null);
        return new Field(name, new FieldType(false, new ArrowType.List(), null), List.of(item));
    }

    private static Field listOfListOfInt32(String name) {
        Field inner = new Field("item", new FieldType(true, I32, null), null);
        Field outerItem = new Field("item",
                new FieldType(true, new ArrowType.List(), null), List.of(inner));
        return new Field(name, new FieldType(false, new ArrowType.List(), null), List.of(outerItem));
    }

    private static Field listOfNullableUtf8(String name) {
        Field item = new Field("item", new FieldType(true, UTF8, null), null);
        return new Field(name, new FieldType(false, new ArrowType.List(), null), List.of(item));
    }

    private static Field listOfBinary(String name) {
        Field item = new Field("item", new FieldType(true, BINARY, null), null);
        return new Field(name, new FieldType(false, new ArrowType.List(), null), List.of(item));
    }

    private static Field mapField(String name) {
        Field key = new Field("key", new FieldType(false, UTF8, null), null);
        Field val = new Field("value", new FieldType(true, UTF8, null), null);
        Field entries = new Field("entries", new FieldType(false, new ArrowType.Struct(), null),
                List.of(key, val));
        return new Field(name, new FieldType(false, new ArrowType.Map(false), null), List.of(entries));
    }

    private static void populate(Map<String, FieldVector> v, TableInfo info) {
        setVarChar(v, "comment", info.comment());
        setMap(v, "tags", info.tags());
        setVarChar(v, "name", info.name());
        setVarChar(v, "schema_name", info.schema_name());
        setVarBinary(v, "columns", info.columns());
        setListInt32(v, "not_null_constraints", info.not_null_constraints());
        setListListInt32(v, "unique_constraints", info.unique_constraints());
        setListUtf8(v, "check_constraints", info.check_constraints());
        setListListInt32(v, "primary_key_constraints", info.primary_key_constraints());
        setListBinary(v, "foreign_key_constraints", info.foreign_key_constraints());
        setBool(v, "supports_insert", info.supports_insert());
        setBool(v, "supports_update", info.supports_update());
        setBool(v, "supports_delete", info.supports_delete());
        setBool(v, "supports_returning", info.supports_returning());
        setBool(v, "supports_column_statistics", info.supports_column_statistics());
        setVarBinary(v, "scan_function", info.scan_function());
        setVarBinary(v, "insert_function", info.insert_function());
        setVarBinary(v, "update_function", info.update_function());
        setVarBinary(v, "delete_function", info.delete_function());
        setInt64Sentinel(v, "cardinality_estimate", info.cardinality_estimate());
        setInt64Sentinel(v, "cardinality_max", info.cardinality_max());
        setVarBinary(v, "column_statistics", info.column_statistics());
        setVarBinary(v, "bind_result", info.bind_result());
    }

    private static void setVarChar(Map<String, FieldVector> v, String f, String value) {
        VarCharVector vec = (VarCharVector) v.get(f);
        if (value == null) vec.setNull(0); else vec.setSafe(0, new Text(value));
    }

    private static void setVarBinary(Map<String, FieldVector> v, String f, byte[] value) {
        VarBinaryVector vec = (VarBinaryVector) v.get(f);
        vec.setSafe(0, value == null ? new byte[0] : value);
    }

    private static void setNullableVarBinary(Map<String, FieldVector> v, String f, byte[] value) {
        VarBinaryVector vec = (VarBinaryVector) v.get(f);
        if (value == null) vec.setNull(0); else vec.setSafe(0, value);
    }

    private static void setBool(Map<String, FieldVector> v, String f, boolean value) {
        ((BitVector) v.get(f)).setSafe(0, value ? 1 : 0);
    }

    private static void setNullableInt64(Map<String, FieldVector> v, String f, Long value) {
        BigIntVector vec = (BigIntVector) v.get(f);
        if (value == null) vec.setNull(0); else vec.setSafe(0, value);
    }

    /** {@code -1} sentinel = "no inlined cardinality" (matches the C++ extension). */
    private static void setInt64Sentinel(Map<String, FieldVector> v, String f, Long value) {
        BigIntVector vec = (BigIntVector) v.get(f);
        vec.setSafe(0, value == null ? -1L : value);
    }

    private static void setListInt32(Map<String, FieldVector> v, String f, List<Integer> values) {
        ListVector vec = (ListVector) v.get(f);
        UnionListWriter w = vec.getWriter();
        w.startList();
        if (values != null) for (Integer i : values) if (i != null) w.integer().writeInt(i);
        w.endList();
        w.setValueCount(1);
    }

    private static void setListListInt32(Map<String, FieldVector> v, String f, List<List<Integer>> values) {
        ListVector vec = (ListVector) v.get(f);
        UnionListWriter w = vec.getWriter();
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

    private static void setListUtf8(Map<String, FieldVector> v, String f, List<String> values) {
        ListVector vec = (ListVector) v.get(f);
        UnionListWriter w = vec.getWriter();
        w.startList();
        if (values != null) for (String s : values) if (s != null) w.varChar().writeVarChar(s);
        w.endList();
        w.setValueCount(1);
    }

    private static void setListBinary(Map<String, FieldVector> v, String f, List<byte[]> values) {
        ListVector vec = (ListVector) v.get(f);
        UnionListWriter w = vec.getWriter();
        w.startList();
        if (values != null) {
            BufferAllocator alloc = Allocators.root();
            for (byte[] b : values) {
                if (b == null) continue;
                org.apache.arrow.memory.ArrowBuf buf = alloc.buffer(b.length);
                buf.writeBytes(b);
                w.varBinary().writeVarBinary(0, b.length, buf);
                buf.close();
            }
        }
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
