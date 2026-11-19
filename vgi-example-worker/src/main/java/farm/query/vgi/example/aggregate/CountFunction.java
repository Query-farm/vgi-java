// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.aggregate.StateCodec;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;

/** {@code vgi_count() -> int64} — nullary aggregate counting rows per group. */
public final class CountFunction implements AggregateFunction<CountFunction.State> {

    public static final class State {
        long count;
    }

    @Override public byte[] serializeState(State s) {
        return StateCodec.encode(out -> {
            out.writeLong(s.count);
        });
    }

    @Override public State deserializeState(byte[] bytes) {
        return StateCodec.decode(bytes, in -> {
            State s = new State();
            s.count = in.readLong();
            return s;
        });
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("result", Schemas.INT64)));

    private static final FunctionSpec SPEC = FunctionSpec.builder("vgi_count")
            .description("Count rows")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        for (long gid : groupIds) {
            states.computeIfAbsent(gid, k -> new State()).count++;
        }
    }

    @Override
    public void combine(State target, State source) { target.count += source.count; }

    @Override
    public void finalize(FieldVector result, int rowIndex, State state) {
        ((BigIntVector) result).setSafe(rowIndex, state.count);
    }

    /** COUNT on empty input returns 0, not NULL. */
    @Override
    public void finalizeEmpty(FieldVector result, int rowIndex) {
        ((BigIntVector) result).setSafe(rowIndex, 0L);
    }
}
