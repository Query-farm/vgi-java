// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionSpec;
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

    private static final FunctionSpec SPEC = FunctionSpec.builder("vgi_sum_all")
            .description("Sum all numeric columns")
            .arg(new ArgSpec(
                "values", 0, Schemas.FLOAT64, "", false, false, "",
                List.of(TypeBoundPredicate.IS_ADDABLE), /*varargs=*/true,
                /*anyType=*/true, /*tableInput=*/false))
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
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
    public void finalize(FieldVector result, int rowIndex, State state) {
        ((Float8Vector) result).setSafe(rowIndex, state.total);
    }
}
