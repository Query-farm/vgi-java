// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.VarCharVector;

/**
 * {@code passthru(value: utf8) -> utf8}. Identity: returns the input unchanged.
 * Zero compute, so a payload sweep over it measures pure round-trip wire cost.
 */
public final class PassthruFunction extends ScalarFn {

    private static final int OFFSET_WIDTH = 4;

    @Override public String name() { return "passthru"; }
    @Override public String description() { return "Returns the input string unchanged (zero-compute wire probe)"; }

    public void compute(@Vector VarCharVector value, VarCharVector result) {
        int rows = value.getValueCount();
        long dataBytes = rows == 0 ? 0L
                : value.getOffsetBuffer().getInt((long) rows * OFFSET_WIDTH);
        result.allocateNew(dataBytes, rows);
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            byte[] b = value.get(i);
            result.setSafe(i, b, 0, b.length);
        }
        result.setValueCount(rows);
    }
}
