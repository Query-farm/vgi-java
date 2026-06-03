// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.NullHandling;
import farm.query.vgi.function.Stability;
import farm.query.vgi.scalar.OutputLength;
import farm.query.vgi.scalar.ScalarFn;
import org.apache.arrow.vector.BitVector;

import java.util.concurrent.ThreadLocalRandom;

/** {@code bernoulli() -> BOOLEAN}, VOLATILE — emits a random bit per row. */
public final class BernoulliFunction extends ScalarFn {

    @Override public String name() { return "bernoulli"; }
    @Override public FunctionMetadata metadata() {
        return new FunctionMetadata(
                "Generate random booleans (demonstrates VOLATILE stability)",
                Stability.VOLATILE, NullHandling.DEFAULT, false, false, false, false);
    }

    public void compute(@OutputLength int rows, BitVector result) {
        for (int i = 0; i < rows; i++) result.setSafe(i, ThreadLocalRandom.current().nextBoolean() ? 1 : 0);
    }
}
