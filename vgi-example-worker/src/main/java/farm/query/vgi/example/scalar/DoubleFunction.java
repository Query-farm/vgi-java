// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.TypeBoundPredicate;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.TypeRules;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;

/**
 * {@code double(value)} — returns {@code value * 2}, promoted one width up
 * for integers (TINYINT→SMALLINT→INTEGER→BIGINT) and float-stable for floats.
 */
public final class DoubleFunction extends ScalarFn {

    @Override public String name() { return "double"; }
    @Override public String description() { return "Doubles numeric values"; }

    @Override
    protected ArrowType outputType(Schema inputSchema, Arguments arguments) {
        if (inputSchema == null || inputSchema.getFields().isEmpty()) return new ArrowType.Null();
        ArrowType in = inputSchema.getFields().get(0).getType();
        // Reject non-multipliable types at bind so the test pins the failure
        // to the bind layer rather than a downstream Arrow kernel exception.
        if (in instanceof ArrowType.Date
                || in instanceof ArrowType.Time
                || in instanceof ArrowType.Timestamp
                || in instanceof ArrowType.Interval
                || in instanceof ArrowType.Duration
                || in instanceof ArrowType.Bool
                || in instanceof ArrowType.Utf8
                || in instanceof ArrowType.LargeUtf8
                || in instanceof ArrowType.Binary
                || in instanceof ArrowType.LargeBinary) {
            throw new IllegalArgumentException(
                    "double: _is_multipliable_type rejects " + in
                            + " — only numeric types (int/float/decimal) are supported");
        }
        return TypeRules.promoteForAddition(in);
    }

    public void compute(
            @Vector(any = true, typeBound = TypeBoundPredicate.IS_ADDABLE) FieldVector value,
            FieldVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            switch (result) {
                case Float8Vector f -> f.setSafe(i, ScalarHelpers.toDouble(value, i) * 2);
                case Float4Vector f -> f.setSafe(i, (float) (ScalarHelpers.toDouble(value, i) * 2));
                case BigIntVector b -> b.setSafe(i, ScalarHelpers.toLong(value, i) * 2);
                case IntVector iv -> iv.setSafe(i, (int) (ScalarHelpers.toLong(value, i) * 2));
                case SmallIntVector s -> s.setSafe(i, (short) (ScalarHelpers.toLong(value, i) * 2));
                case TinyIntVector t -> t.setSafe(i, (byte) (ScalarHelpers.toLong(value, i) * 2));
                case org.apache.arrow.vector.UInt8Vector u -> u.setSafe(i, ScalarHelpers.toLong(value, i) * 2);
                case org.apache.arrow.vector.UInt4Vector u -> u.setSafe(i, (int) (ScalarHelpers.toLong(value, i) * 2));
                case org.apache.arrow.vector.UInt2Vector u -> u.setSafe(i, (int) (ScalarHelpers.toLong(value, i) * 2));
                case org.apache.arrow.vector.UInt1Vector u -> u.setSafe(i, (int) (ScalarHelpers.toLong(value, i) * 2));
                case DecimalVector d -> {
                    BigDecimal bd = ((DecimalVector) value).getObject(i);
                    BigDecimal doubled = bd.add(bd);
                    int precision = ((ArrowType.Decimal) d.getField().getType()).getPrecision();
                    if (doubled.precision() > precision) {
                        throw new IllegalArgumentException(
                                "value " + doubled + " does not fit in precision " + precision);
                    }
                    d.setSafe(i, doubled);
                }
                default -> throw new IllegalStateException("unexpected output type: " + result.getField().getType());
            }
        }
    }
}
