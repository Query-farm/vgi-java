// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * Two overloads of {@code concat_values}:
 * <ul>
 *   <li>{@code (BIGINT...)} — sums the integer columns and returns the result as a string.</li>
 *   <li>{@code (VARCHAR...)} — concatenates the string columns.</li>
 * </ul>
 */
public final class ConcatValuesFunctions {

    private ConcatValuesFunctions() {}

    /** Catalog-enumeration: advertise ANY when input schema is absent; concrete VARCHAR otherwise. */
    private static ArrowType outputUtf8OrAny(Schema in) {
        if (in == null || in.getFields().isEmpty()) return new ArrowType.Null();
        return Schemas.UTF8;
    }

    public static final class IntVariant extends ScalarFn {
        @Override public String name() { return "concat_values"; }
        @Override public String description() { return "Sum integer varargs and return as string"; }

        @Override protected ArrowType outputType(Schema inputSchema, Arguments arguments) {
            return outputUtf8OrAny(inputSchema);
        }

        public void compute(
                @Vector(varargs = true) List<BigIntVector> values,
                VarCharVector result) {
            int rows = values.get(0).getValueCount();
            for (int r = 0; r < rows; r++) {
                boolean anyNull = false;
                long sum = 0;
                for (BigIntVector c : values) {
                    if (c.isNull(r)) { anyNull = true; break; }
                    sum += c.get(r);
                }
                if (anyNull) result.setNull(r);
                else result.setSafe(r, new Text(Long.toString(sum)));
            }
        }
    }

    public static final class StrVariant extends ScalarFn {
        @Override public String name() { return "concat_values"; }
        @Override public String description() { return "Concatenate string varargs"; }

        @Override protected ArrowType outputType(Schema inputSchema, Arguments arguments) {
            return outputUtf8OrAny(inputSchema);
        }

        public void compute(
                @Vector(varargs = true) List<VarCharVector> values,
                VarCharVector result) {
            // Byte-level concat: the inputs are already contiguous UTF-8 in their
            // Arrow data buffers, so we copy raw bytes and never round-trip
            // through java.lang.String (which would decode UTF-8 -> UTF-16 and
            // re-encode, doubling the data and burning the transcoder). One pass
            // sizes the output, a second copies; pre-allocating means setSafe
            // never reallocates + recopies a growing buffer.
            int rows = values.get(0).getValueCount();
            int ncols = values.size();

            long totalBytes = 0;
            int maxRowLen = 0;
            for (int r = 0; r < rows; r++) {
                boolean anyNull = false;
                int rowLen = 0;
                for (int c = 0; c < ncols; c++) {
                    VarCharVector v = values.get(c);
                    if (v.isNull(r)) { anyNull = true; break; }
                    rowLen += v.getValueLength(r);
                }
                if (!anyNull) {
                    totalBytes += rowLen;
                    if (rowLen > maxRowLen) maxRowLen = rowLen;
                }
            }

            result.allocateNew(totalBytes, rows);
            byte[] scratch = new byte[maxRowLen];
            for (int r = 0; r < rows; r++) {
                boolean anyNull = false;
                for (int c = 0; c < ncols; c++) {
                    if (values.get(c).isNull(r)) { anyNull = true; break; }
                }
                if (anyNull) {
                    result.setNull(r);
                    continue;
                }
                int pos = 0;
                for (int c = 0; c < ncols; c++) {
                    VarCharVector v = values.get(c);
                    int start = v.getOffsetBuffer().getInt((long) r * VarCharVector.OFFSET_WIDTH);
                    int len = v.getValueLength(r);
                    v.getDataBuffer().getBytes(start, scratch, pos, len);
                    pos += len;
                }
                result.setSafe(r, scratch, 0, pos);
            }
            result.setValueCount(rows);
        }
    }
}
