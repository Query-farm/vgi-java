// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import farm.query.vgi.catalog.Macro;

/**
 * Encodes a macro's named-parameter default values as a 1-row IPC RecordBatch
 * whose columns are the named parameters and whose row 0 holds the typed
 * default values. The default-value SQL strings in {@link Macro} are
 * parsed loosely: {@code 0}/{@code 100} → BIGINT, decimals → FLOAT64,
 * {@code 'foo'} → VARCHAR, {@code true}/{@code false} → BOOL. Anything else
 * falls back to VARCHAR with the literal text.
 */
public final class MacroDefaultsEncoder {

    private MacroDefaultsEncoder() {}

    public static byte[] encode(Map<String, String> defaults) {
        if (defaults == null || defaults.isEmpty()) return new byte[0];
        Map<String, Object> values = new LinkedHashMap<>();
        for (var e : defaults.entrySet()) values.put(e.getKey(), parse(e.getValue()));

        BufferAllocator alloc = Allocators.root();
        List<Field> fields = new ArrayList<>();
        for (var e : values.entrySet()) {
            fields.add(new Field(e.getKey(),
                    new FieldType(true, arrowTypeFor(e.getValue()), null), null));
        }
        Schema schema = new Schema(fields);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
            root.allocateNew();
            int col = 0;
            for (var e : values.entrySet()) {
                FieldVector v = root.getVector(col++);
                writeScalar(v, e.getValue());
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
            throw new RuntimeException("MacroDefaultsEncoder failed", e);
        }
    }

    private static Object parse(String literal) {
        if (literal == null) return null;
        String s = literal.trim();
        if (s.isEmpty()) return "";
        if (s.equalsIgnoreCase("true")) return Boolean.TRUE;
        if (s.equalsIgnoreCase("false")) return Boolean.FALSE;
        if (s.startsWith("'") && s.endsWith("'") && s.length() >= 2) {
            return s.substring(1, s.length() - 1);
        }
        try { return Long.parseLong(s); } catch (NumberFormatException ignore) {}
        try { return Double.parseDouble(s); } catch (NumberFormatException ignore) {}
        return s;
    }

    private static ArrowType arrowTypeFor(Object v) {
        if (v instanceof Boolean) return new ArrowType.Bool();
        if (v instanceof Long) return new ArrowType.Int(64, true);
        if (v instanceof Double) return new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        return new ArrowType.Utf8();
    }

    private static void writeScalar(FieldVector v, Object value) {
        if (v instanceof BitVector b) b.setSafe(0, ((Boolean) value) ? 1 : 0);
        else if (v instanceof BigIntVector bi) bi.setSafe(0, ((Number) value).longValue());
        else if (v instanceof Float8Vector f) f.setSafe(0, ((Number) value).doubleValue());
        else if (v instanceof VarCharVector vc) vc.setSafe(0, new Text(value == null ? "" : value.toString()));
    }
}
