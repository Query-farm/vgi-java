// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** {@code vgi_count() -> int64} — nullary aggregate counting rows per group. */
public final class CountFunction implements AggregateFunction<CountFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        long count;
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
    public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
        ((BigIntVector) output.getVector("result")).setSafe(rowIndex, state.count);
    }

    /** COUNT on empty input returns 0, not NULL. */
    @Override
    public void finalizeEmpty(VectorSchemaRoot output, int rowIndex) {
        ((BigIntVector) output.getVector("result")).setSafe(rowIndex, 0L);
    }
}
