// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.VarCharVector;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/** {@code upper_case(value: utf8) -> utf8}. */
public final class UpperCaseFunction extends ScalarFn {

    private static final int OFFSET_WIDTH = 4;  // utf8 offsets are int32

    @Override public String name() { return "upper_case"; }
    @Override public String description() { return "Converts string values to uppercase"; }

    public void compute(@Vector VarCharVector value, VarCharVector result) {
        int rows = value.getValueCount();
        // Uppercasing is length-preserving for ASCII, so the input's used data
        // size is a tight upper bound for the common case; presizing avoids the
        // repeated grow-and-copy that setSafe() would otherwise do as the data
        // buffer fills.
        long dataBytes = rows == 0 ? 0L
                : value.getOffsetBuffer().getInt((long) rows * OFFSET_WIDTH);
        result.allocateNew(dataBytes, rows);
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            byte[] up = upper(value.get(i));
            result.setSafe(i, up, 0, up.length);
        }
    }

    /**
     * Uppercase UTF-8 bytes. ASCII inputs are uppercased in a single pass with
     * no decode; non-ASCII falls back to a Unicode-correct, locale-independent
     * transform. {@code Locale.ROOT} is required — the no-arg
     * {@code String.toUpperCase()} is locale-sensitive (e.g. the Turkish
     * dotless-i), which would make the result depend on the worker's host
     * locale.
     */
    private static byte[] upper(byte[] in) {
        boolean ascii = true;
        for (byte b : in) {
            if (b < 0) { ascii = false; break; }
        }
        if (ascii) {
            byte[] out = new byte[in.length];
            for (int i = 0; i < in.length; i++) {
                byte b = in[i];
                out[i] = (b >= 'a' && b <= 'z') ? (byte) (b - 32) : b;
            }
            return out;
        }
        return new String(in, StandardCharsets.UTF_8)
                .toUpperCase(Locale.ROOT)
                .getBytes(StandardCharsets.UTF_8);
    }
}
