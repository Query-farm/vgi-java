// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

/**
 * {@code format_number} — three overloads dispatched by arity:
 * <ol>
 *   <li>{@code format_number(value)} → {@code "%.0f"}</li>
 *   <li>{@code format_number(precision, value)} → {@code "%.<precision>f"}</li>
 *   <li>{@code format_number(precision, prefix, value)} → {@code "<prefix>%.<precision>f"}</li>
 * </ol>
 */
public final class FormatNumberFunctions {

    private FormatNumberFunctions() {}

    private static void format(Float8Vector value, int precision, String prefix, VarCharVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            String formatted = String.format("%." + precision + "f", value.get(i));
            result.setSafe(i, new Text(prefix + formatted));
        }
    }

    public static final class Default extends ScalarFn {
        @Override public String name() { return "format_number"; }
        @Override public String description() { return "Format number with default precision (0 decimals)"; }

        public void compute(@Vector Float8Vector value, VarCharVector result) {
            format(value, 0, "", result);
        }
    }

    public static final class WithPrecision extends ScalarFn {
        @Override public String name() { return "format_number"; }
        @Override public String description() { return "Format number with specified precision"; }

        public void compute(
                @Const long precision,
                @Vector Float8Vector value,
                VarCharVector result) {
            format(value, (int) precision, "", result);
        }
    }

    public static final class Full extends ScalarFn {
        @Override public String name() { return "format_number"; }
        @Override public String description() { return "Format number with precision and prefix"; }

        public void compute(
                @Const long precision,
                @Const String prefix,
                @Vector Float8Vector value,
                VarCharVector result) {
            format(value, (int) precision, prefix == null ? "" : prefix, result);
        }
    }
}
