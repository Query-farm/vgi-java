// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.TypeBoundPredicate;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** {@code vgi_sum_all(values...) -> DOUBLE} — variadic numeric aggregate. */
public final class SumAllFunction implements AggregateFunction<SumAllFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        double total;
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("result", Schemas.FLOAT64)));

    @Override public String name() { return "vgi_sum_all"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Sum all numeric inputs (variadic)");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec(
                "values", 0, Schemas.FLOAT64, "", false, false, "",
                List.of(TypeBoundPredicate.IS_ADDABLE), /*varargs=*/true,
                /*anyType=*/false, /*tableInput=*/false));
    }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    @Override
    public Schema bindOutputSchema(Schema inputSchema) {
        // Catalog enumeration passes null; the actual zero-arg case at bind
        // sends an empty inputSchema — reject that explicitly.
        if (inputSchema != null && inputSchema.getFields().isEmpty()) {
            throw new IllegalArgumentException("vgi_sum_all requires at least 1 value");
        }
        return OUTPUT_SCHEMA;
    }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        int rows = input.getRowCount();
        List<FieldVector> cols = input.getFieldVectors();
        for (int i = 0; i < rows; i++) {
            boolean any = false;
            double sum = 0;
            for (FieldVector c : cols) {
                if (c.isNull(i)) continue;
                any = true;
                sum += ScalarHelpers.toDouble(c, i);
            }
            if (!any) continue;
            State s = states.computeIfAbsent(groupIds[i], k -> new State());
            s.total += sum;
        }
    }

    @Override
    public void combine(State target, State source) { target.total += source.total; }

    @Override
    public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
        ((Float8Vector) output.getVector("result")).setSafe(rowIndex, state.total);
    }
}
