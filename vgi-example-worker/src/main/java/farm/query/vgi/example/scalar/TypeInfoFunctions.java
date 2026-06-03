// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code type_info(value)} — five overloads that each accept a different Arrow
 * type and return the type name as VARCHAR. Verifies column-type-based dispatch.
 */
public final class TypeInfoFunctions {

    private TypeInfoFunctions() {}

    private static void fillLabel(FieldVector value, VarCharVector result, String label) {
        Text txt = new Text(label);
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) result.setNull(i);
            else result.setSafe(i, txt);
        }
    }

    private static abstract class Base extends ScalarFn {
        private final ArrowType inputType;
        private final String label;
        private final String descriptionLabel;

        Base(ArrowType inputType, String label) { this(inputType, label, label); }
        Base(ArrowType inputType, String label, String descriptionLabel) {
            this.inputType = inputType;
            this.label = label;
            this.descriptionLabel = descriptionLabel;
        }

        @Override public final String name() { return "type_info"; }
        @Override public final String description() { return "Return type name for " + descriptionLabel + " input"; }

        @Override
        public final List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("v", 0, inputType));
        }

        // Subclasses override compute() with a concrete @Vector type — the
        // framework finds it reflectively. Each variant calls the shared helper.
        protected final String label() { return label; }
    }

    public static final class Int32 extends Base {
        public Int32() { super(Schemas.INT32, "int32"); }
        public void compute(@Vector org.apache.arrow.vector.IntVector v, VarCharVector result) {
            fillLabel(v, result, label());
        }
    }
    public static final class Int64 extends Base {
        public Int64() { super(Schemas.INT64, "int64"); }
        public void compute(@Vector org.apache.arrow.vector.BigIntVector v, VarCharVector result) {
            fillLabel(v, result, label());
        }
    }
    public static final class UInt32 extends Base {
        public UInt32() { super(Schemas.UINT32, "uint32"); }
        // UInt32Vector isn't in vectorClassToArrow; argumentSpecs() override covers the wire shape.
        // Use the generic FieldVector parameter, accept any actual class at runtime.
        public void compute(@Vector(any = true) FieldVector v, VarCharVector result) {
            fillLabel(v, result, label());
        }
    }
    public static final class UInt64 extends Base {
        public UInt64() { super(Schemas.UINT64, "uint64"); }
        public void compute(@Vector(any = true) FieldVector v, VarCharVector result) {
            fillLabel(v, result, label());
        }
    }
    public static final class Varchar extends Base {
        public Varchar() { super(Schemas.UTF8, "varchar", "string"); }
        public void compute(@Vector VarCharVector v, VarCharVector result) {
            fillLabel(v, result, label());
        }
    }
}
