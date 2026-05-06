// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
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

import java.util.List;

/**
 * Two overloads of {@code concat_values}:
 * <ul>
 *   <li>{@code (BIGINT...)} — sums the integer columns and returns the
 *       result as a string.</li>
 *   <li>{@code (VARCHAR...)} — concatenates the string columns.</li>
 * </ul>
 * Used by overload/scalar_varargs_overload.test to verify type-based
 * dispatch on a varargs scalar function.
 */
public final class ConcatValuesFunctions {

    private ConcatValuesFunctions() {}

    public static final class IntVariant implements ScalarFunction {
        @Override public String name() { return "concat_values"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Sum integer varargs and return as string");
        }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec(
                    "values", 0, Schemas.INT64, "", false, false, "",
                    List.of(), /*varargs=*/true, /*anyType=*/false));
        }
        @Override public BindResponse onBind(ScalarBindParams p) {
            return BindResponse.forSchema(Schemas.singleResultIpc(Schemas.UTF8));
        }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            int rows = input.getRowCount();
            List<FieldVector> cols = input.getFieldVectors();
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector v = (VarCharVector) out.getVector("result");
            for (int r = 0; r < rows; r++) {
                boolean anyNull = false;
                long sum = 0;
                for (FieldVector c : cols) {
                    if (c.isNull(r)) { anyNull = true; break; }
                    sum += ScalarHelpers.toLong(c, r);
                }
                if (anyNull) v.setNull(r);
                else v.setSafe(r, new Text(Long.toString(sum)));
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class StrVariant implements ScalarFunction {
        @Override public String name() { return "concat_values"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Concatenate string varargs");
        }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec(
                    "values", 0, Schemas.UTF8, "", false, false, "",
                    List.of(), /*varargs=*/true, /*anyType=*/false));
        }
        @Override public BindResponse onBind(ScalarBindParams p) {
            return BindResponse.forSchema(Schemas.singleResultIpc(Schemas.UTF8));
        }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            int rows = input.getRowCount();
            List<FieldVector> cols = input.getFieldVectors();
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector v = (VarCharVector) out.getVector("result");
            for (int r = 0; r < rows; r++) {
                boolean anyNull = false;
                StringBuilder sb = new StringBuilder();
                for (FieldVector c : cols) {
                    if (c.isNull(r)) { anyNull = true; break; }
                    sb.append(((VarCharVector) c).getObject(r).toString());
                }
                if (anyNull) v.setNull(r);
                else v.setSafe(r, new Text(sb.toString()));
            }
            out.setRowCount(rows);
            return out;
        }
    }
}
