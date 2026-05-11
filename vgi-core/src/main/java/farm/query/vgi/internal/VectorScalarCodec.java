// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified scalar reader/writer for Arrow {@link FieldVector}s.
 *
 * <p>Read returns Java values normalised for downstream consumption:
 * integer widths upcast to {@code Long}, float widths upcast to {@code Double},
 * struct → {@code LinkedHashMap}, list/fixed-size-list → {@code ArrayList}.
 * Struct children are always included regardless of the struct's null bit
 * (callers needing the strict "skip null children" shape use
 * {@code SettingsParser.parse} directly).</p>
 *
 * <p>Write handles the same vector classes plus the unsigned-int and
 * date/time families used by {@code AttachOptionDefaultMaterializer}.</p>
 */
public final class VectorScalarCodec {

    private VectorScalarCodec() {}

    /**
     * Read row {@code row} out of {@code v} as a Java value.
     *
     * <p>Returns {@code null} when the vector reports the cell as null,
     * except for {@link StructVector}s — struct null bits are ignored and
     * children are recursed into. Falls back to {@code v.getObject(row)}
     * for unrecognised vector classes.</p>
     */
    public static Object read(FieldVector v, int row) {
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
            return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
        }
        if (v instanceof LargeVarCharVector vc) {
            byte[] bytes = vc.get(row);
            return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
        }
        if (v instanceof VarBinaryVector vb) return vb.get(row);
        if (v instanceof LargeVarBinaryVector vb) return vb.get(row);
        if (v instanceof DecimalVector d) return d.getObject(row);
        if (v instanceof Decimal256Vector d) return d.getObject(row);
        if (v instanceof DateDayVector d) return (long) d.get(row);
        if (v instanceof StructVector sv) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Field f : sv.getField().getChildren()) {
                FieldVector child = sv.getChild(f.getName());
                out.put(f.getName(), read(child, row));
            }
            return out;
        }
        if (v instanceof ListVector lv) {
            int start = lv.getElementStartIndex(row);
            int end = lv.getElementEndIndex(row);
            FieldVector inner = lv.getDataVector();
            List<Object> out = new ArrayList<>(end - start);
            for (int i = start; i < end; i++) out.add(read(inner, i));
            return out;
        }
        if (v instanceof FixedSizeListVector fl) {
            int width = fl.getListSize();
            int start = row * width;
            FieldVector inner = fl.getDataVector();
            List<Object> out = new ArrayList<>(width);
            for (int i = 0; i < width; i++) out.add(read(inner, start + i));
            return out;
        }
        return v.getObject(row);
    }

    /**
     * Write {@code value} into row {@code row} of {@code v}.
     *
     * <p>Recurses into struct (Map values) and list (List values). Setting
     * {@code value == null} writes the null bit. The struct child write
     * always calls {@link StructVector#setIndexDefined} so the struct itself
     * is reported as non-null; if no children are populated, the parent
     * struct vector is treated as a record of nulls rather than a null
     * struct — callers needing distinct {@code null} struct semantics
     * should pre-call {@link StructVector#setNull} explicitly.</p>
     */
    @SuppressWarnings("unchecked")
    public static void write(FieldVector v, int row, Object value) {
        if (value == null) { v.setNull(row); return; }
        if (v instanceof BitVector x) x.setSafe(row, ((Boolean) value) ? 1 : 0);
        else if (v instanceof TinyIntVector x) x.setSafe(row, ((Number) value).byteValue());
        else if (v instanceof SmallIntVector x) x.setSafe(row, ((Number) value).shortValue());
        else if (v instanceof IntVector x) x.setSafe(row, ((Number) value).intValue());
        else if (v instanceof BigIntVector x) x.setSafe(row, ((Number) value).longValue());
        else if (v instanceof UInt1Vector x) x.setSafe(row, ((Number) value).byteValue());
        else if (v instanceof UInt2Vector x) x.setSafe(row, (char) ((Number) value).intValue());
        else if (v instanceof UInt4Vector x) x.setSafe(row, ((Number) value).intValue());
        else if (v instanceof UInt8Vector x) x.setSafe(row, ((Number) value).longValue());
        else if (v instanceof Float4Vector x) x.setSafe(row, ((Number) value).floatValue());
        else if (v instanceof Float8Vector x) x.setSafe(row, ((Number) value).doubleValue());
        else if (v instanceof VarCharVector x) x.setSafe(row, new Text(value.toString()));
        else if (v instanceof VarBinaryVector x) x.setSafe(row, (byte[]) value);
        else if (v instanceof DateDayVector x) x.setSafe(row, ((Number) value).intValue());
        else if (v instanceof TimeMicroVector x) x.setSafe(row, ((Number) value).longValue());
        else if (v instanceof TimeStampMicroVector x) x.setSafe(row, ((Number) value).longValue());
        else if (v instanceof TimeStampMicroTZVector x) x.setSafe(row, ((Number) value).longValue());
        else if (v instanceof DecimalVector x) {
            BigDecimal bd = value instanceof BigDecimal b ? b : new BigDecimal(value.toString());
            x.setSafe(row, bd.setScale(x.getScale(), RoundingMode.HALF_UP));
        } else if (v instanceof ListVector lv) {
            writeList(lv, row, (List<Object>) value);
        } else if (v instanceof StructVector sv) {
            writeStruct(sv, row, (Map<String, Object>) value);
        } else {
            throw new IllegalArgumentException(
                    "VectorScalarCodec.write: unsupported vector " + v.getClass().getSimpleName());
        }
    }

    private static void writeList(ListVector lv, int row, List<Object> items) {
        int startOffset = lv.startNewValue(row);
        FieldVector data = lv.getDataVector();
        int needed = startOffset + items.size();
        while (data.getValueCapacity() < needed) data.reAlloc();
        for (int i = 0; i < items.size(); i++) write(data, startOffset + i, items.get(i));
        lv.endValue(row, items.size());
        if (needed > data.getValueCount()) data.setValueCount(needed);
    }

    private static void writeStruct(StructVector sv, int row, Map<String, Object> values) {
        for (Field f : sv.getField().getChildren()) {
            write(sv.getChild(f.getName()), row, values.get(f.getName()));
        }
        sv.setIndexDefined(row);
    }
}
