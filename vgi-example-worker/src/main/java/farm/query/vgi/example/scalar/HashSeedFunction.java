// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.OutputLength;
import farm.query.vgi.scalar.ScalarFn;
import org.apache.arrow.vector.BigIntVector;

/** {@code hash_seed(seed: int64 [const]) -> int64}: emits {@code seed + row_index}. */
public final class HashSeedFunction extends ScalarFn {

    @Override public String name() { return "hash_seed"; }
    @Override public String description() { return "Generate deterministic integers from a constant seed"; }

    public void compute(
            @Const long seed,
            @OutputLength int rows,
            BigIntVector result) {
        for (int i = 0; i < rows; i++) result.setSafe(i, seed + i);
    }
}
