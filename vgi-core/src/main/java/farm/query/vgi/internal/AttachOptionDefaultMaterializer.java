// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * Builds a one-row {@link FieldVector} carrying an ATTACH option's default
 * value. Called <em>once</em> per spec at worker registration; on the merge
 * hot path the resulting vector is copied via {@code TransferPair} so this
 * is the only place Java→Arrow type dispatch lives.
 */
public final class AttachOptionDefaultMaterializer {

    private AttachOptionDefaultMaterializer() {}

    /** Allocate a length-1 vector for {@code field} and write {@code value}
     *  into row 0. Caller owns the returned vector. */
    public static FieldVector materialize(Field field, Object value) {
        FieldVector v = field.createVector(Allocators.root());
        v.allocateNew();
        writeAt(v, 0, value);
        v.setValueCount(1);
        return v;
    }

    @SuppressWarnings("unchecked")
    private static void writeAt(FieldVector v, int row, Object value) {
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
                    "AttachOptionDefaultMaterializer: unsupported vector "
                            + v.getClass().getSimpleName());
        }
    }

    private static void writeList(ListVector lv, int row, List<Object> items) {
        int startOffset = lv.startNewValue(row);
        FieldVector data = lv.getDataVector();
        int needed = startOffset + items.size();
        while (data.getValueCapacity() < needed) data.reAlloc();
        for (int i = 0; i < items.size(); i++) {
            writeAt(data, startOffset + i, items.get(i));  // recurse — no second cascade
        }
        lv.endValue(row, items.size());
        if (needed > data.getValueCount()) data.setValueCount(needed);
    }

    private static void writeStruct(StructVector sv, int row, Map<String, Object> values) {
        for (Field f : sv.getField().getChildren()) {
            writeAt(sv.getChild(f.getName()), row, values.get(f.getName()));
        }
        sv.setIndexDefined(row);
    }
}
