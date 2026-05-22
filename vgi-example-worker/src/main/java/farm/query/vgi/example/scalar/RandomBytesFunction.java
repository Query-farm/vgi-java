// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.OutputLength;
import farm.query.vgi.scalar.ScalarFn;
import org.apache.arrow.vector.VarBinaryVector;

import java.util.Random;

/** {@code random_bytes(seed BIGINT [const], byte_length BIGINT [const]) -> BLOB} — deterministic. */
public final class RandomBytesFunction extends ScalarFn {

    @Override public String name() { return "random_bytes"; }
    @Override public String description() { return "Generate pseudo-random binary blobs from seed and length"; }

    public void compute(
            @Const long seed,
            @Const(value = "byte_length") long byteLength,
            @OutputLength int rows,
            VarBinaryVector result) {
        if (byteLength < 0) throw new IllegalArgumentException("byte_length must be >= 0");
        Random rng = new Random(seed);
        int len = (int) byteLength;
        for (int i = 0; i < rows; i++) {
            byte[] buf = new byte[len];
            rng.nextBytes(buf);
            result.setSafe(i, buf);
        }
    }
}
