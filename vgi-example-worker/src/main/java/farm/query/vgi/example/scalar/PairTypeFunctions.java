// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code pair_type(a, b)} — three overloads dispatched by the column types of
 * the two inputs: {@code (int+int)}, {@code (str+str)}, {@code (int+str)}.
 */
public final class PairTypeFunctions {

    private PairTypeFunctions() {}

    private static final byte[] OUT = Schemas.singleResultIpc(Schemas.UTF8);

    private static abstract class Base implements ScalarFunction {
        private final ArrowType ta;
        private final ArrowType tb;
        private final String label;

        Base(ArrowType ta, ArrowType tb, String label) { this.ta = ta; this.tb = tb; this.label = label; }

        @Override public final String name() { return "pair_type"; }
        @Override public final FunctionMetadata metadata() {
            return FunctionMetadata.describe("Return type pair name for " + label);
        }
        @Override public final List<ArgSpec> argumentSpecs() {
            return List.of(
                    new ArgSpec("a", 0, ta),
                    new ArgSpec("b", 1, tb));
        }
        @Override public final BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public final VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                          BufferAllocator alloc) {
            int rows = input.getRowCount();
            FieldVector a = input.getFieldVectors().get(0);
            FieldVector b = input.getFieldVectors().get(1);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            Text txt = new Text(label);
            for (int i = 0; i < rows; i++) {
                if (a.isNull(i) || b.isNull(i)) r.setNull(i);
                else r.setSafe(i, txt);
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class IntInt extends Base { public IntInt() { super(Schemas.INT64, Schemas.INT64, "int+int"); } }
    public static final class StrStr extends Base { public StrStr() { super(Schemas.UTF8, Schemas.UTF8, "str+str"); } }
    public static final class IntStr extends Base { public IntStr() { super(Schemas.INT64, Schemas.UTF8, "int+str"); } }
}
