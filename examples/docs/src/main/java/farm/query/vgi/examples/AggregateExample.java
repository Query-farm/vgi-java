// VGI-Java example: an aggregate function.
//
// An aggregate collapses many rows into one value per group. VGI aggregates are
// built for DuckDB's *parallel, partial* aggregation model, so you implement
// four pieces:
//
//   newState()  — a fresh, empty accumulator
//   update()    — fold a batch of rows into the per-group accumulators
//   combine()   — merge two partial accumulators (parallel workers / spill)
//   finalize()  — write a group's accumulator out as the result value
//
// The `State` is `Serializable` because partials may cross process boundaries
// when DuckDB parallelizes the aggregation. Keep it small.
//
//   ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:/abs/path/bin/runAggregate');
//   SELECT g, demo.vgi_sum(v) FROM (VALUES (1,10),(1,20),(2,5)) t(g,v) GROUP BY g;
//   -- 1 -> 30, 2 -> 5
package farm.query.vgi.examples;

import farm.query.vgi.Worker;
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

/** {@code vgi_sum(value BIGINT) -> BIGINT}: sum per group, overflow-checked. */
public final class AggregateExample implements AggregateFunction<AggregateExample.State> {

    /** Per-group accumulator. Serializable: partials may be merged across workers. */
    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        long total;
    }

    private static final Schema OUTPUT_SCHEMA =
            new Schema(List.of(Schemas.nullable("result", Schemas.INT64)));

    private static final FunctionSpec SPEC = FunctionSpec.builder("vgi_sum")
            .description("Sum integer values")
            .arg("value", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    // Fold one input batch into the accumulators. `groupIds[i]` is the group of
    // row i; states.computeIfAbsent mints an accumulator the first time a group
    // is seen in this partition.
    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        FieldVector v = input.getFieldVectors().get(0);
        if (!(v instanceof BigIntVector b)) return;
        int rows = input.getRowCount();
        try {
            for (int i = 0; i < rows; i++) {
                if (b.isNull(i)) continue;
                State s = states.computeIfAbsent(groupIds[i], k -> new State());
                s.total = Math.addExact(s.total, b.get(i));
            }
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("vgi_sum: int64 overflow", e);
        }
    }

    // Merge a partial (`source`) produced by another worker into `target`.
    @Override
    public void combine(State target, State source) {
        target.total = Math.addExact(target.total, source.total);
    }

    // Write one group's final value into the output column at `rowIndex`.
    @Override
    public void finalize(FieldVector result, int rowIndex, State state) {
        ((BigIntVector) result).setSafe(rowIndex, state.total);
    }

    public static void main(String[] args) {
        Worker.builder()
                .catalogName("demo")
                .registerAggregate(new AggregateExample())
                .runFromArgs(args);
    }
}
