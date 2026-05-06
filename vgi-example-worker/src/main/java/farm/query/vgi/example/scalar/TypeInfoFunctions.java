// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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
 * {@code type_info(value)} — five overloads that each accept a different Arrow
 * type and return the type name as a VARCHAR. Used by
 * overload/scalar_overload.test to verify column-type-based dispatch.
 */
public final class TypeInfoFunctions {

    private TypeInfoFunctions() {}

    private static final byte[] OUT = Schemas.singleResultIpc(Schemas.UTF8);

    private static abstract class Base implements ScalarFunction {
        private final ArrowType inputType;
        private final String label;

        Base(ArrowType inputType, String label) { this.inputType = inputType; this.label = label; }

        @Override public final String name() { return "type_info"; }
        @Override public final FunctionMetadata metadata() {
            return FunctionMetadata.describe("Return type name for " + label + " input");
        }
        @Override public final List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("v", 0, inputType));
        }
        @Override public final BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public final VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                          BufferAllocator alloc) {
            int rows = input.getRowCount();
            FieldVector v = input.getFieldVectors().get(0);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            Text txt = new Text(label);
            for (int i = 0; i < rows; i++) {
                if (v.isNull(i)) r.setNull(i);
                else r.setSafe(i, txt);
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class Int32 extends Base { public Int32() { super(Schemas.INT32, "int32"); } }
    public static final class Int64 extends Base { public Int64() { super(Schemas.INT64, "int64"); } }
    public static final class UInt32 extends Base { public UInt32() { super(Schemas.UINT32, "uint32"); } }
    public static final class UInt64 extends Base { public UInt64() { super(Schemas.UINT64, "uint64"); } }
    public static final class Varchar extends Base { public Varchar() { super(Schemas.UTF8, "varchar"); } }
}
