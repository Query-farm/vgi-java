// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.TypeBoundPredicate;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.TypeRules;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

/** {@code sum_values(values...)} — varargs numeric column sum, promoted output type. */
public final class SumValuesFunction implements ScalarFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("sum_values")
            .description("Sum multiple numeric values")
            .arg(new ArgSpec(
                    "values", 0, new ArrowType.Null(), "", false, false, "",
                    List.of(TypeBoundPredicate.IS_ADDABLE), /*varargs=*/true, /*anyType=*/true))
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

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
        List<FieldVector> cols = input.getFieldVectors();
        return ScalarHelpers.mapNumericRows(params.outputSchema(), alloc, cols, input.getRowCount(),
                i -> { long s = 0; for (FieldVector c : cols) s += ScalarHelpers.toLong(c, i); return s; },
                i -> { double s = 0; for (FieldVector c : cols) s += ScalarHelpers.toDouble(c, i); return s; });
    }
}
