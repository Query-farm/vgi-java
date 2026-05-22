// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Encodes a {@code ScanFunctionResult} (function_name + nested args batch +
 * required_extensions) as a 1-row IPC stream. The C++ extension reads this
 * from {@code TableInfo.scan_function} and skips the per-bind
 * {@code catalog_table_scan_function_get} RPC.
 */
public final class ScanFunctionResultEncoder {

    private ScanFunctionResultEncoder() {}

    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();

    public static byte[] encode(String functionName, List<Object> positional,
                                  Map<String, Object> named, List<String> requiredExtensions) {
        BufferAllocator alloc = Allocators.root();
        Schema schema = new Schema(List.of(
                new Field("function_name", new FieldType(false, UTF8, null), null),
                new Field("arguments", new FieldType(false, BINARY, null), null),
                new Field("required_extensions",
                        new FieldType(false, new ArrowType.List(), null),
                        List.of(new Field("item", new FieldType(true, UTF8, null), null)))));

        byte[] argsBytes = encodeArguments(positional, named, alloc);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
            root.allocateNew();
            ((VarCharVector) root.getVector("function_name")).setSafe(0, new Text(functionName));
            ((VarBinaryVector) root.getVector("arguments")).setSafe(0, argsBytes);
            ListVector ext = (ListVector) root.getVector("required_extensions");
            UnionListWriter w = ext.getWriter();
            w.startList();
            if (requiredExtensions != null) {
                for (String s : requiredExtensions) if (s != null) w.varChar().writeVarChar(s);
            }
            w.endList();
            w.setValueCount(1);
            root.setRowCount(1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter sw = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                sw.start();
                sw.writeBatch();
                sw.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("ScanFunctionResult encode failed", e);
        }
    }

    /** Public entry point used by {@code catalog_table_scan_function_get}. */
    public static byte[] encodeArguments(List<Object> positional, Map<String, Object> named) {
        return encodeArguments(positional, named, Allocators.root());
    }

    private static byte[] encodeArguments(List<Object> positional, Map<String, Object> named,
                                            BufferAllocator alloc) {
        List<Field> fields = new ArrayList<>();
        if (positional != null) {
            for (int i = 0; i < positional.size(); i++) {
                fields.add(new Field("arg_" + i,
                        new FieldType(true, arrowTypeFor(positional.get(i)), null), null));
            }
        }
        if (named != null) {
            for (var e : named.entrySet()) {
                fields.add(new Field(e.getKey(),
                        new FieldType(true, arrowTypeFor(e.getValue()), null), null));
            }
        }
        Schema argsSchema = new Schema(fields);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(argsSchema, alloc)) {
            root.allocateNew();
            int col = 0;
            if (positional != null) {
                for (Object v : positional) {
                    writeScalar(root.getVector(col++), v);
                }
            }
            if (named != null) {
                for (Object v : named.values()) {
                    writeScalar(root.getVector(col++), v);
                }
            }
            root.setRowCount(1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("scan_function arguments encode failed", e);
        }
    }

    private static ArrowType arrowTypeFor(Object v) {
        if (v == null) return UTF8;
        if (v instanceof Boolean) return new ArrowType.Bool();
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return new ArrowType.Int(64, true);
        }
        if (v instanceof Double || v instanceof Float) {
            return new ArrowType.FloatingPoint(
                    org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        }
        if (v instanceof CharSequence) return UTF8;
        if (v instanceof byte[]) return BINARY;
        return UTF8;
    }

    private static void writeScalar(FieldVector v, Object value) {
        if (value == null) { v.setNull(0); return; }
        if (v instanceof org.apache.arrow.vector.BitVector b) {
            b.setSafe(0, ((Boolean) value) ? 1 : 0);
        } else if (v instanceof org.apache.arrow.vector.BigIntVector bi) {
            bi.setSafe(0, ((Number) value).longValue());
        } else if (v instanceof org.apache.arrow.vector.Float8Vector f) {
            f.setSafe(0, ((Number) value).doubleValue());
        } else if (v instanceof VarCharVector vc) {
            vc.setSafe(0, new Text(value.toString()));
        } else if (v instanceof VarBinaryVector vb) {
            vb.setSafe(0, (byte[]) value);
        }
    }
}
