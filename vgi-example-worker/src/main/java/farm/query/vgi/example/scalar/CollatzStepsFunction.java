// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;

/** {@code collatz_steps(n INT64) -> INT64}. Data-dependent CPU loop. */
public final class CollatzStepsFunction extends ScalarFn {

    @Override public String name() { return "collatz_steps"; }
    @Override public String description() { return "Number of Collatz (3n+1) steps to reach 1"; }

    public void compute(@Vector(doc = "Positive integer") BigIntVector value, BigIntVector result) {
        int rows = value.getValueCount();
        if (result.getValueCapacity() < rows) {
            result.allocateNew(rows);
        }
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            long n = value.get(i);
            long steps = 0;
            if (n > 0) {
                while (n != 1) {
                    n = (n & 1L) == 0 ? n / 2 : 3 * n + 1;
                    steps++;
                }
            }
            result.set(i, steps);
        }
        result.setValueCount(rows);
    }
}
