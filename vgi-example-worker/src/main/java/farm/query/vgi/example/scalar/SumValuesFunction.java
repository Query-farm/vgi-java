// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.TypeBoundPredicate;
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

/** {@code sum_values(values...)} — varargs numeric column sum, promoted output type. */
public final class SumValuesFunction extends ScalarFn {

    @Override public String name() { return "sum_values"; }
    @Override public String description() { return "Sum multiple numeric values"; }

    @Override
    protected ArrowType outputType(Schema inputSchema, Arguments arguments) {
        if (inputSchema == null) return new ArrowType.Null();
        if (inputSchema.getFields().isEmpty()) {
            throw new IllegalArgumentException("sum_values requires at least 1 value");
        }
        ArrowType widest = inputSchema.getFields().get(0).getType();
        for (int i = 1; i < inputSchema.getFields().size(); i++) {
            ArrowType t = inputSchema.getFields().get(i).getType();
            if (TypeRules.isFloating(t)) widest = t;
            else if (widest instanceof ArrowType.Int wi && t instanceof ArrowType.Int ti
                    && ti.getBitWidth() > wi.getBitWidth()) widest = t;
        }
        return TypeRules.promoteForAddition(widest);
    }

    public void compute(
            @Vector(any = true, varargs = true, typeBound = TypeBoundPredicate.IS_ADDABLE) List<FieldVector> values,
            FieldVector result) {
        int rows = values.get(0).getValueCount();
        for (int i = 0; i < rows; i++) {
            boolean anyNull = false;
            for (FieldVector c : values) if (c.isNull(i)) { anyNull = true; break; }
            if (anyNull) { result.setNull(i); continue; }
            switch (result) {
                case BigIntVector b -> {
                    long s = 0;
                    for (FieldVector c : values) s += ScalarHelpers.toLong(c, i);
                    b.setSafe(i, s);
                }
                case Float8Vector f -> {
                    double s = 0;
                    for (FieldVector c : values) s += ScalarHelpers.toDouble(c, i);
                    f.setSafe(i, s);
                }
                default -> throw new IllegalStateException("unexpected output type: " + result.getField().getType());
            }
        }
    }
}
