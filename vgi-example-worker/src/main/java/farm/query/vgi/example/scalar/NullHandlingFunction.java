// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.NullHandling;
import farm.query.vgi.function.Stability;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;

/** {@code null_handling(value: int64) -> int64}: returns the input or {@code -5000} when NULL. */
public final class NullHandlingFunction extends ScalarFn {

    @Override public String name() { return "null_handling"; }
    @Override public FunctionMetadata metadata() {
        return new FunctionMetadata(
                "Returns value or -5000 if null",
                Stability.CONSISTENT, NullHandling.SPECIAL, false, false, false, false);
    }

    public void compute(@Vector BigIntVector value, BigIntVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            result.setSafe(i, value.isNull(i) ? -5000L : value.get(i));
        }
    }
}
