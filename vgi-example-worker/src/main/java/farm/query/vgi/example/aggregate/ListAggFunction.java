// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** {@code vgi_listagg(value: utf8) -> utf8}: concatenate strings with comma separator. */
public final class ListAggFunction implements AggregateFunction<ListAggFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        ArrayList<String> items = new ArrayList<>();
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("result", new FieldType(true, Schemas.UTF8, null), null)));

    @Override public String name() { return "vgi_listagg"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Concatenate strings with comma separator");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec("value", 0, Schemas.UTF8));
    }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        FieldVector v = input.getFieldVectors().get(0);
        if (!(v instanceof VarCharVector vc)) return;
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            if (vc.isNull(i)) continue;
            State s = states.computeIfAbsent(groupIds[i], k -> new State());
            s.items.add(ScalarHelpers.toString(vc, i));
        }
    }

    @Override
    public void combine(State target, State source) {
        target.items.addAll(source.items);
    }

    @Override
    public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
        VarCharVector v = (VarCharVector) output.getVector("result");
        v.setSafe(rowIndex, new Text(String.join(",", state.items)));
    }
}
