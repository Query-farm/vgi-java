// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.VarCharVector;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * {@code hash_rounds(s: utf8, rounds INT64 [const]) -> utf8}. Apply SHA-256
 * {@code rounds} times (key-stretching); {@code rounds} is the const compute knob.
 */
public final class HashRoundsFunction extends ScalarFn {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    @Override public String name() { return "hash_rounds"; }
    @Override public String description() { return "Apply SHA-256 rounds times (key-stretching); rounds is a const compute knob"; }

    public void compute(
            @Vector(doc = "String to stretch") VarCharVector value,
            @Const(doc = "Number of SHA-256 rounds") long rounds,
            VarCharVector result) {
        int rows = value.getValueCount();
        int k = (int) Math.max(0, rounds);
        result.allocateNew((long) rows * 64, rows);
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            byte[] buf = value.get(i);
            for (int r = 0; r < k; r++) {
                md.reset();
                buf = md.digest(buf);
            }
            byte[] out = new byte[buf.length * 2];
            for (int j = 0; j < buf.length; j++) {
                out[j * 2] = (byte) HEX[(buf[j] >> 4) & 0xf];
                out[j * 2 + 1] = (byte) HEX[buf[j] & 0xf];
            }
            result.setSafe(i, out, 0, out.length);
        }
        result.setValueCount(rows);
    }
}
