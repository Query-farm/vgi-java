// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

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

/**
 * {@code add_values(col1, col2)} — sum two numeric columns. Uses "any"-typed
 * arguments with the {@code IS_ADDABLE} type bound: DuckDB matches any numeric
 * input pair, and {@code onBind} picks the promoted output type via
 * {@link TypeRules#commonTypeForAddition}.
 */
public final class AddValuesFunction implements ScalarFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("add_values")
            .description("Adds two numeric values")
            .any("col1", TypeBoundPredicate.IS_ADDABLE)
            .any("col2", TypeBoundPredicate.IS_ADDABLE)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(ScalarBindParams params) {
        // Catalog enumeration calls onBind without an input_schema — emit
        // ArrowType.Null which DuckDB renders as "ANY" in duckdb_functions().
        if (params.inputSchema() == null || params.inputSchema().getFields().isEmpty()) {
            return BindResponse.forSchema(Schemas.singleResultAnyIpc());
        }
        ArrowType a = inputType(params, 0);
        ArrowType b = inputType(params, 1);
        ArrowType out = TypeRules.commonTypeForAddition(a, b);
        return BindResponse.forSchema(Schemas.singleResultIpc(out));
    }

    private static ArrowType inputType(ScalarBindParams params, int idx) {
        if (params.inputSchema() == null) return Schemas.INT64;
        if (params.inputSchema().getFields().size() <= idx) return Schemas.INT64;
        return params.inputSchema().getFields().get(idx).getType();
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        FieldVector left = input.getFieldVectors().get(0);
        FieldVector right = input.getFieldVectors().get(1);
        return ScalarHelpers.mapNumericRows(params.outputSchema(), alloc,
                input.getFieldVectors(), input.getRowCount(),
                i -> ScalarHelpers.toLong(left, i) + ScalarHelpers.toLong(right, i),
                i -> ScalarHelpers.toDouble(left, i) + ScalarHelpers.toDouble(right, i));
    }
}
