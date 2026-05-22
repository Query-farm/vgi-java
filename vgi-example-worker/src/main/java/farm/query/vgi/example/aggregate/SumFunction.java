// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** {@code vgi_sum(value: int64) -> int64} aggregate. */
public final class SumFunction implements AggregateFunction<SumFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        long total;
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("result", Schemas.INT64)));

    private static final FunctionSpec SPEC = FunctionSpec.builder("vgi_sum")
            .description("Sum integer values")
            .arg("value", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        FieldVector v = input.getFieldVectors().get(0);
        if (!(v instanceof BigIntVector b)) return;
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            if (b.isNull(i)) continue;
            long gid = groupIds[i];
            State s = states.computeIfAbsent(gid, k -> new State());
            s.total += b.get(i);
        }
    }

    @Override
    public void combine(State target, State source) { target.total += source.total; }

    @Override
    public void finalize(FieldVector result, int rowIndex, State state) {
        ((BigIntVector) result).setSafe(rowIndex, state.total);
    }
}
