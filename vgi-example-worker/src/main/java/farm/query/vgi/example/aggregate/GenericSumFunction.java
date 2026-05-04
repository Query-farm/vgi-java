// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.TypeBoundPredicate;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.TypeRules;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * {@code vgi_generic_sum(value: ANY) -> ANY} — sum that resolves return type
 * from the input type at bind time.
 */
public final class GenericSumFunction implements AggregateFunction<GenericSumFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        double total;
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("result", new FieldType(true, Schemas.FLOAT64, null), null)));

    @Override public String name() { return "vgi_generic_sum"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Sum that resolves return type from input at bind time");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.any("value", 0, List.of(TypeBoundPredicate.IS_ADDABLE)));
    }
    /**
     * Catalog enumeration uses this; the actual emit type is decided at
     * aggregate_bind from the input schema. The framework wires the output
     * schema through to {@link #finalize}, so the static value here is just
     * the placeholder for catalog enumeration.
     */
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override
    public Schema bindOutputSchema(Schema inputSchema) {
        if (inputSchema == null || inputSchema.getFields().isEmpty()) return OUTPUT_SCHEMA;
        ArrowType inType = inputSchema.getFields().get(0).getType();
        return new Schema(java.util.List.of(
                new Field("result", new FieldType(true, inType, null), null)));
    }
    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        FieldVector v = input.getFieldVectors().get(0);
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            if (v.isNull(i)) continue;
            State s = states.computeIfAbsent(groupIds[i], k -> new State());
            s.total += ScalarHelpers.toDouble(v, i);
        }
    }

    @Override
    public void combine(State target, State source) { target.total += source.total; }

    @Override
    public void finalize(VectorSchemaRoot output, int rowIndex, State state) {
        FieldVector v = output.getVector("result");
        ArrowType t = v.getField().getType();
        if (TypeRules.isFloating(t)) {
            ((Float8Vector) v).setSafe(rowIndex, state.total);
        } else if (v instanceof BigIntVector b) {
            b.setSafe(rowIndex, (long) state.total);
        } else if (v instanceof IntVector i) {
            i.setSafe(rowIndex, (int) state.total);
        } else {
            v.setNull(rowIndex);
        }
    }
}
