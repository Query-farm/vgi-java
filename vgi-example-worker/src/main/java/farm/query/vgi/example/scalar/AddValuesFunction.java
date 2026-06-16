// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.TypeBoundPredicate;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.TypeRules;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** {@code add_values(col1, col2)} — sum two numeric columns, promoted output. */
public final class AddValuesFunction extends ScalarFn {

    @Override public String name() { return "add_values"; }
    @Override public String description() { return "Adds two numeric values"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(description()).withExamples(List.of(
                new FunctionExample(
                        "SELECT add_values(a, b) FROM (VALUES (1, 2), (3, 4)) t(a, b)",
                        "Add two integer columns row-wise.",
                        "3\n7"),
                new FunctionExample(
                        "SELECT add_values(1.5, 2.5)",
                        "Add two floating-point literals (promoted output).",
                        null)));
    }

    @Override
    protected ArrowType outputType(Schema inputSchema, Arguments arguments) {
        if (inputSchema == null || inputSchema.getFields().size() < 2) return new ArrowType.Null();
        ArrowType a = inputSchema.getFields().get(0).getType();
        ArrowType b = inputSchema.getFields().get(1).getType();
        return TypeRules.commonTypeForAddition(a, b);
    }

    public void compute(
            @Vector(any = true, typeBound = TypeBoundPredicate.IS_ADDABLE) FieldVector col1,
            @Vector(any = true, typeBound = TypeBoundPredicate.IS_ADDABLE) FieldVector col2,
            FieldVector result) {
        int rows = col1.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (col1.isNull(i) || col2.isNull(i)) { result.setNull(i); continue; }
            switch (result) {
                case BigIntVector b -> b.setSafe(i, ScalarHelpers.toLong(col1, i) + ScalarHelpers.toLong(col2, i));
                case Float8Vector f -> f.setSafe(i, ScalarHelpers.toDouble(col1, i) + ScalarHelpers.toDouble(col2, i));
                default -> throw new IllegalStateException("unexpected output type: " + result.getField().getType());
            }
        }
    }
}
