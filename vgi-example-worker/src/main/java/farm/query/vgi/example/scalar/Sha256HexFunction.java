// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.VarCharVector;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** {@code sha256_hex(s: utf8) -> utf8}. Lowercase hex SHA-256 per row. */
public final class Sha256HexFunction extends ScalarFn {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    @Override public String name() { return "sha256_hex"; }
    @Override public String description() { return "Lowercase hex SHA-256 of the UTF-8 string"; }

    public void compute(@Vector(doc = "String to hash") VarCharVector value, VarCharVector result) {
        int rows = value.getValueCount();
        result.allocateNew((long) rows * 64, rows);
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] hex = new byte[64];
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            md.reset();
            byte[] d = md.digest(value.get(i));
            for (int j = 0; j < 32; j++) {
                hex[j * 2] = (byte) HEX[(d[j] >> 4) & 0xf];
                hex[j * 2 + 1] = (byte) HEX[d[j] & 0xf];
            }
            result.setSafe(i, hex, 0, 64);
        }
        result.setValueCount(rows);
    }
}
