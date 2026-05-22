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
            int rows = values.get(0).getValueCount();
            for (int r = 0; r < rows; r++) {
                boolean anyNull = false;
                StringBuilder sb = new StringBuilder();
                for (VarCharVector c : values) {
                    if (c.isNull(r)) { anyNull = true; break; }
                    sb.append(c.getObject(r).toString());
                }
                if (anyNull) result.setNull(r);
                else result.setSafe(r, new Text(sb.toString()));
            }
        }
    }
}
