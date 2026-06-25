// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;

/** {@code multiply(value INT64, factor INT64 [const]) -> INT64}. */
public final class MultiplyFunction extends ScalarFn {

    @Override public String name() { return "multiply"; }
    @Override public String description() { return "Multiplies a value by a constant factor"; }

    public void compute(
            @Vector(doc = "Integer value to multiply") BigIntVector value,
            @Const(doc = "Multiplication factor") long factor,
            BigIntVector result) {
        int rows = value.getValueCount();

        // (1) Reuse the framework's allocation when its capacity is enough.
        //
        // The vgi-rpc-java dispatcher already calls outRoot.allocateNew()
        // before invoking compute(). Calling result.allocateNew(rows) here
        // does clear()+allocateBytes() — releasing the framework's buffer to
        // Netty's PooledByteBufAllocator and re-acquiring an equivalent one.
        // Fast (O(1) arena lookup) but not free. Skip when the existing
        // capacity already covers this batch — the common case for steady-
        // state pipelines where every batch is the same vector size.
        if (result.getValueCapacity() < rows) {
            result.allocateNew(rows);
        }

        // (2) Direct ArrowBuf access bypasses BigIntVector.set()'s per-row
        // BitVectorHelper.setBit() (a byte read-modify-write on every row).
        // Plain wrapping `*` matches Arrow C++ semantics; the multiplyExact
        // JIT intrinsic is cheap so removing it alone didn't move the needle.
        ArrowBuf srcData = value.getDataBuffer();
        ArrowBuf dstData = result.getDataBuffer();
        for (int i = 0; i < rows; i++) {
            dstData.setLong((long) i * 8L, srcData.getLong((long) i * 8L) * factor);
        }

        // (3) Validity bitmap: nulls propagate (output null iff input null).
        // Two fast paths:
        //   - input has zero nulls (the common case): mark all output rows
        //     valid with one bulk byte fill — no need to read the input bitmap.
        //   - input has nulls: bulk-memcpy via ArrowBuf.setBytes (a memcpy
        //     under the hood), not the byte-at-a-time loop the prior version
        //     used.
        int validityBytes = (rows + 7) / 8;
        ArrowBuf dstValid = result.getValidityBuffer();
        if (value.getNullCount() == 0) {
            int wholeBytes = rows / 8;
            for (int i = 0; i < wholeBytes; i++) {
                dstValid.setByte(i, (byte) 0xFF);
            }
            int trailingBits = rows - wholeBytes * 8;
            if (trailingBits > 0) {
                dstValid.setByte(wholeBytes, (byte) ((1 << trailingBits) - 1));
            }
        } else {
            ArrowBuf srcValid = value.getValidityBuffer();
            dstValid.setBytes(0L, srcValid, 0L, (long) validityBytes);
        }
        result.setValueCount(rows);
    }
}
