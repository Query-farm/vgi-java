// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.NullHandling;
import farm.query.vgi.function.Stability;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;

import java.util.concurrent.ThreadLocalRandom;

/** {@code random_int(min_val: int64, max_val: int64) -> int64}, VOLATILE. */
public final class RandomIntFunction extends ScalarFn {

    @Override public String name() { return "random_int"; }
    @Override public FunctionMetadata metadata() {
        return new FunctionMetadata(
                "Generate random integers (demonstrates VOLATILE stability)",
                Stability.VOLATILE, NullHandling.DEFAULT, false, false, false, false);
    }

    public void compute(
            @Vector(value = "min_val") BigIntVector minV,
            @Vector(value = "max_val") BigIntVector maxV,
            BigIntVector result) {
        int rows = minV.getValueCount();
        for (int i = 0; i < rows; i++) {
            long lo = minV.get(i);
            long hi = maxV.get(i);
            if (hi <= lo) { result.setSafe(i, lo); continue; }
            // hi + 1 overflows for hi = Long.MAX_VALUE; nextLong requires bound > origin.
            long v = hi == Long.MAX_VALUE
                    ? ThreadLocalRandom.current().nextLong(lo, hi)
                    : ThreadLocalRandom.current().nextLong(lo, hi + 1);
            result.setSafe(i, v);
        }
    }
}
