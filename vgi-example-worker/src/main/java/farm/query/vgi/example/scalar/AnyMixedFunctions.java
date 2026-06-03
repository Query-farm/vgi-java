// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.ScalarHelpers;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

/**
 * {@code any_mixed(a, b)} — two overloads where {@code a} accepts any Arrow
 * type and {@code b} is dispatched on type ({@code int64} vs {@code utf8}).
 */
public final class AnyMixedFunctions {

    private AnyMixedFunctions() {}

    public static final class IntVariant extends ScalarFn {
        @Override public String name() { return "any_mixed"; }
        @Override public String description() { return "Any+int dispatch"; }

        public void compute(
                @Vector(any = true) FieldVector a,
                @Vector BigIntVector b,
                VarCharVector result) {
            int rows = a.getValueCount();
            for (int i = 0; i < rows; i++) {
                if (a.isNull(i) || b.isNull(i)) result.setNull(i);
                else result.setSafe(i, new Text("any+int: " + b.get(i)));
            }
        }
    }

    public static final class StrVariant extends ScalarFn {
        @Override public String name() { return "any_mixed"; }
        @Override public String description() { return "Any+str dispatch"; }

        public void compute(
                @Vector(any = true) FieldVector a,
                @Vector VarCharVector b,
                VarCharVector result) {
            int rows = a.getValueCount();
            for (int i = 0; i < rows; i++) {
                if (a.isNull(i) || b.isNull(i)) result.setNull(i);
                else result.setSafe(i, new Text("any+str: " + b.getObject(i)));
            }
        }
    }

    public static final class SmartFormatInt extends ScalarFn {
        @Override public String name() { return "smart_format"; }
        @Override public String description() { return "Right-align value in field of given width"; }

        public void compute(
                @Const long width,
                @Vector Float8Vector value,
                VarCharVector result) {
            int rows = value.getValueCount();
            for (int i = 0; i < rows; i++) {
                if (value.isNull(i)) result.setNull(i);
                else result.setSafe(i, new Text(String.format("%" + width + "s", value.get(i))));
            }
        }
    }

    public static final class SmartFormatStr extends ScalarFn {
        @Override public String name() { return "smart_format"; }
        @Override public String description() { return "Prepend prefix to formatted value"; }

        public void compute(
                @Const String prefix,
                @Vector Float8Vector value,
                VarCharVector result) {
            int rows = value.getValueCount();
            String safePrefix = prefix == null ? "null" : prefix;
            for (int i = 0; i < rows; i++) {
                if (value.isNull(i)) result.setNull(i);
                else result.setSafe(i, new Text(safePrefix + value.get(i)));
            }
        }
    }
}
