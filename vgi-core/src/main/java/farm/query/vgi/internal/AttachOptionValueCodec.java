// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Read/write Java values into a {@link FieldVector} at a given row index.
 * Covers every Arrow type the {@code attach_options_echo} test exercises:
 * bool, int8/16/32/64, uint8/16/32/64, float32/64, utf8/binary, date32,
 * time64[us], timestamp[us] (with/without tz), decimal128, list, struct.
 */
public final class AttachOptionValueCodec {

    private AttachOptionValueCodec() {}

    /** Write {@code value} into vector {@code v} at row {@code row}. */
    @SuppressWarnings({"unchecked"})
    public static void writeValue(FieldVector v, int row, Object value) {
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
        } else if (v instanceof Decimal256Vector x) {
            BigDecimal bd = value instanceof BigDecimal b ? b : new BigDecimal(value.toString());
            x.setSafe(row, bd.setScale(x.getScale(), RoundingMode.HALF_UP));
        } else if (v instanceof ListVector lv) {
            writeList(lv, row, (List<Object>) value);
        } else if (v instanceof StructVector sv) {
            Map<String, Object> map = (Map<String, Object>) value;
            for (org.apache.arrow.vector.types.pojo.Field f : sv.getField().getChildren()) {
                writeValue(sv.getChild(f.getName()), row, map.get(f.getName()));
            }
            sv.setIndexDefined(row);
        } else {
            throw new IllegalArgumentException(
                    "AttachOptionValueCodec: unsupported vector " + v.getClass().getSimpleName());
        }
    }

    private static void writeList(ListVector lv, int row, List<Object> items) {
        UnionListWriter w = lv.getWriter();
        w.setPosition(row);
        w.startList();
        for (Object item : items) writeListItem(w, item, lv.getAllocator());
        w.endList();
    }

    @SuppressWarnings("unchecked")
    private static void writeListItem(BaseWriter.ListWriter w, Object item, BufferAllocator alloc) {
        if (item == null) { w.writeNull(); return; }
        if (item instanceof Long l) w.bigInt().writeBigInt(l);
        else if (item instanceof Integer i) w.bigInt().writeBigInt(i.longValue());
        else if (item instanceof Double d) w.float8().writeFloat8(d);
        else if (item instanceof Boolean b) w.bit().writeBit(b ? 1 : 0);
        else if (item instanceof String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            try (ArrowBuf buf = alloc.buffer(bytes.length)) {
                buf.setBytes(0, bytes);
                w.varChar().writeVarChar(0, bytes.length, buf);
            }
        } else if (item instanceof List<?>) {
            BaseWriter.ListWriter inner = w.list();
            inner.startList();
            for (Object child : (List<Object>) item) writeListItem(inner, child, alloc);
            inner.endList();
        } else {
            throw new IllegalArgumentException("list element of unsupported type: " + item.getClass());
        }
    }

    /** Read a Java value out of vector {@code v} at row {@code row}.
     *  Used when echoing user-provided options back through {@code attach_id}. */
    public static Object readValue(FieldVector v, int row) {
        if (v.isNull(row)) return null;
        if (v instanceof BitVector x) return x.get(row) != 0;
        if (v instanceof TinyIntVector x) return x.get(row);
        if (v instanceof SmallIntVector x) return x.get(row);
        if (v instanceof IntVector x) return x.get(row);
        if (v instanceof BigIntVector x) return x.get(row);
        if (v instanceof UInt1Vector x) return x.get(row);
        if (v instanceof UInt2Vector x) return (int) x.get(row);
        if (v instanceof UInt4Vector x) return x.get(row);
        if (v instanceof UInt8Vector x) return x.get(row);
        if (v instanceof Float4Vector x) return x.get(row);
        if (v instanceof Float8Vector x) return x.get(row);
        if (v instanceof VarCharVector x) return new String(x.get(row), StandardCharsets.UTF_8);
        if (v instanceof VarBinaryVector x) return x.get(row);
        if (v instanceof DateDayVector x) return x.get(row);
        if (v instanceof TimeMicroVector x) return x.get(row);
        if (v instanceof TimeStampMicroVector x) return x.get(row);
        if (v instanceof TimeStampMicroTZVector x) return x.get(row);
        if (v instanceof DecimalVector x) return x.getObject(row);
        if (v instanceof Decimal256Vector x) return x.getObject(row);
        if (v instanceof ListVector lv) return lv.getObject(row);
        if (v instanceof StructVector sv) return sv.getObject(row);
        throw new IllegalArgumentException(
                "AttachOptionValueCodec.read: unsupported vector " + v.getClass().getSimpleName());
    }
}
