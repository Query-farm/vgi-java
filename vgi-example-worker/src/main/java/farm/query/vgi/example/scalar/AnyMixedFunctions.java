// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

/**
 * {@code any_mixed(a, b)} — two overloads where {@code a} accepts any Arrow
 * type and {@code b} is dispatched on type ({@code int64} vs {@code utf8}).
 */
public final class AnyMixedFunctions {

    private AnyMixedFunctions() {}

    private static final byte[] OUT = Schemas.singleResultIpc(Schemas.UTF8);

    public static final class IntVariant implements ScalarFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("any_mixed")
                .description("Any+int dispatch")
                .any("a")
                .arg("b", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            int rows = input.getRowCount();
            FieldVector a = input.getFieldVectors().get(0);
            FieldVector b = input.getFieldVectors().get(1);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                if (a.isNull(i) || b.isNull(i)) r.setNull(i);
                else r.setSafe(i, new Text("any+int: " + ScalarHelpers.toLong(b, i)));
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class StrVariant implements ScalarFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("any_mixed")
                .description("Any+str dispatch")
                .any("a")
                .arg("b", Schemas.UTF8)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            int rows = input.getRowCount();
            FieldVector a = input.getFieldVectors().get(0);
            FieldVector b = input.getFieldVectors().get(1);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                if (a.isNull(i) || b.isNull(i)) r.setNull(i);
                else {
                    String s = ((VarCharVector) b).getObject(i).toString();
                    r.setSafe(i, new Text("any+str: " + s));
                }
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class SmartFormatInt implements ScalarFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("smart_format")
                .description("Right-align value in field of given width")
                .constArg("width", Schemas.INT64)
                .arg("value", Schemas.FLOAT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            int width = ((Number) p.arguments().positionalAt(0)).intValue();
            int rows = input.getRowCount();
            FieldVector v = input.getFieldVectors().get(0);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                if (v.isNull(i)) r.setNull(i);
                else r.setSafe(i, new Text(String.format("%" + width + "s", ScalarHelpers.toDouble(v, i))));
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class SmartFormatStr implements ScalarFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("smart_format")
                .description("Prepend prefix to formatted value")
                .constArg("prefix", Schemas.UTF8)
                .arg("value", Schemas.FLOAT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            String prefix = (String) p.arguments().positionalAt(0);
            int rows = input.getRowCount();
            FieldVector v = input.getFieldVectors().get(0);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                if (v.isNull(i)) r.setNull(i);
                else r.setSafe(i, new Text(prefix + ScalarHelpers.toDouble(v, i)));
            }
            out.setRowCount(rows);
            return out;
        }
    }
}
