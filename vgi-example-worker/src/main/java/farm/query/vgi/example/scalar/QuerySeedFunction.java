// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.NullHandling;
import farm.query.vgi.function.Stability;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;

/**
 * {@code query_seed(value: int64) -> int64}, the only fixture that declares
 * {@link Stability#CONSISTENT_WITHIN_QUERY}. The numeric result is fixed (adds
 * a constant 1000) so SQL tests have a stable expected output; the stability
 * flag is what is under test, keeping the third wire-enum variant exercised.
 * Mirrors vgi-python's {@code QuerySeedFunction}.
 */
public final class QuerySeedFunction extends ScalarFn {

    @Override public String name() { return "query_seed"; }

    @Override public FunctionMetadata metadata() {
        return new FunctionMetadata(
                "Add a per-query-stable seed to each value (demonstrates CONSISTENT_WITHIN_QUERY stability)",
                Stability.CONSISTENT_WITHIN_QUERY, NullHandling.DEFAULT, false, false, false, false);
    }

    public void compute(@Vector(value = "value") BigIntVector value, BigIntVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            result.setSafe(i, value.get(i) + 1000L);
        }
    }
}
