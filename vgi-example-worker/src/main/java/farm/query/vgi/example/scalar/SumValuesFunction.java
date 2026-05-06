// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.TypeBoundPredicate;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.TypeRules;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** {@code sum_values(values...)} — varargs numeric column sum, promoted output type. */
public final class SumValuesFunction implements ScalarFunction {

    @Override public String name() { return "sum_values"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Sum multiple numeric values");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec(
                "values", 0, new ArrowType.Null(), "", false, false, "",
                List.of(TypeBoundPredicate.IS_ADDABLE), /*varargs=*/true, /*anyType=*/true));
    }

    @Override public BindResponse onBind(ScalarBindParams params) {
        if (params.inputSchema() == null) {
            return BindResponse.forSchema(Schemas.singleResultAnyIpc());
        }
        if (params.inputSchema().getFields().isEmpty()) {
            throw new IllegalArgumentException("sum_values requires at least 1 value");
        }
        ArrowType widest = params.inputSchema().getFields().get(0).getType();
        for (int i = 1; i < params.inputSchema().getFields().size(); i++) {
            ArrowType t = params.inputSchema().getFields().get(i).getType();
            if (TypeRules.isFloating(t)) widest = t;
            else if (widest instanceof ArrowType.Int wi && t instanceof ArrowType.Int ti
                    && ti.getBitWidth() > wi.getBitWidth()) widest = t;
        }
        return BindResponse.forSchema(Schemas.singleResultIpc(TypeRules.promoteForAddition(widest)));
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        Schema outSchema = params.outputSchema();
        ArrowType outType = outSchema.getFields().get(0).getType();
        int rows = input.getRowCount();
        List<FieldVector> cols = input.getFieldVectors();
        VectorSchemaRoot out = VectorSchemaRoot.create(outSchema, alloc);
        out.allocateNew();
        if (TypeRules.isFloating(outType)) {
            Float8Vector v = (Float8Vector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                boolean anyNull = false;
                double sum = 0;
                for (FieldVector c : cols) {
                    if (c.isNull(i)) { anyNull = true; break; }
                    sum += ScalarHelpers.toDouble(c, i);
                }
                if (anyNull) v.setNull(i);
                else v.setSafe(i, sum);
            }
        } else {
            BigIntVector v = (BigIntVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                boolean anyNull = false;
                long sum = 0;
                for (FieldVector c : cols) {
                    if (c.isNull(i)) { anyNull = true; break; }
                    sum += ScalarHelpers.toLong(c, i);
                }
                if (anyNull) v.setNull(i);
                else v.setSafe(i, sum);
            }
        }
        out.setRowCount(rows);
        return out;
    }
}
