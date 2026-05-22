// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;

/** {@code multiply(value INT64, factor INT64 [const]) -> INT64}. */
public final class MultiplyFunction extends ScalarFn {

    @Override public String name() { return "multiply"; }
    @Override public String description() { return "Multiplies a value by a constant factor"; }

    public void compute(
            @Vector BigIntVector value,
            @Const long factor,
            BigIntVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) result.setNull(i);
            else result.setSafe(i, value.get(i) * factor);
        }
    }
}
