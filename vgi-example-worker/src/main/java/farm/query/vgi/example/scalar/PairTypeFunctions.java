// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/** {@code pair_type(a, b)} — three overloads dispatched by column types. */
public final class PairTypeFunctions {

    private PairTypeFunctions() {}

    private static void fillPairLabel(FieldVector a, FieldVector b, VarCharVector result, String label) {
        Text txt = new Text(label);
        int rows = a.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (a.isNull(i) || b.isNull(i)) result.setNull(i);
            else result.setSafe(i, txt);
        }
    }

    private static abstract class Base extends ScalarFn {
        private final ArrowType ta;
        private final ArrowType tb;
        private final String label;

        Base(ArrowType ta, ArrowType tb, String label) { this.ta = ta; this.tb = tb; this.label = label; }

        @Override public final String name() { return "pair_type"; }
        @Override public final String description() { return "Return type pair name for " + label; }

        @Override
        public final List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("a", 0, ta), new ArgSpec("b", 1, tb));
        }

        protected final String label() { return label; }
    }

    public static final class IntInt extends Base {
        public IntInt() { super(Schemas.INT64, Schemas.INT64, "int+int"); }
        public void compute(@Vector BigIntVector a, @Vector BigIntVector b, VarCharVector result) {
            fillPairLabel(a, b, result, label());
        }
    }
    public static final class StrStr extends Base {
        public StrStr() { super(Schemas.UTF8, Schemas.UTF8, "str+str"); }
        public void compute(@Vector VarCharVector a, @Vector VarCharVector b, VarCharVector result) {
            fillPairLabel(a, b, result, label());
        }
    }
    public static final class IntStr extends Base {
        public IntStr() { super(Schemas.INT64, Schemas.UTF8, "int+str"); }
        public void compute(@Vector BigIntVector a, @Vector VarCharVector b, VarCharVector result) {
            fillPairLabel(a, b, result, label());
        }
    }
}
