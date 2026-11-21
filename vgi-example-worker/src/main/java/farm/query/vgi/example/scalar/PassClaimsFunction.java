// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

import java.util.Map;
import java.util.TreeMap;

/**
 * {@code pass_claims(x: int64) -> utf8}: the caller's verified Orchard Pass claims as a JSON object.
 *
 * <p>Registered on the example worker only in Orchard hosted mode ({@code VGI_ORCHARD_HOSTED=1}),
 * so the extension's function-count tests are unaffected. Mirrors vgi-python's
 * {@code vgi._test_fixtures.orchard_hosted.PassClaimsFunction} and the contract in the vgi
 * repository's {@code test/hosted/README.md}: the object is
 * {@code {"claims": {...}, "domain": ..., "principal": ...}} with sorted keys and Python's
 * {@code json.dumps} separators, and claims keep their JSON types ({@code email_verified} is a
 * boolean and {@code exp} a number), so a test can tell a real boolean from the string
 * {@code "true"}.
 *
 * <p>SQL: {@code SELECT pass_claims(1)}
 */
public final class PassClaimsFunction extends ScalarFn {

    @Override public String name() { return "pass_claims"; }
    @Override public String description() { return "Return the caller's verified Pass claims as a JSON object."; }

    public void compute(@Vector BigIntVector x, VarCharVector result) {
        AuthContext auth = AuthScope.current().auth();
        // The whole verified claim set, as decoded from the Pass (vgi/test/hosted/README.md).
        Map<String, Object> claims = new TreeMap<>(auth.claims());
        Map<String, Object> payload = new TreeMap<>();
        payload.put("principal", auth.principal());
        payload.put("domain", auth.domain());
        payload.put("claims", claims);
        Text json = new Text(toJson(payload));
        int rows = x.getValueCount();
        for (int i = 0; i < rows; i++) result.setSafe(i, json);
        result.setValueCount(rows);
    }

    /** JSON with Python {@code json.dumps(..., sort_keys=True)} formatting. */
    static String toJson(Object value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value);
        return sb.toString();
    }

    private static void write(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof Boolean || v instanceof Integer || v instanceof Long
                || v instanceof Short || v instanceof Byte) {
            sb.append(v);
        } else if (v instanceof Number n) {
            double d = n.doubleValue();
            if (d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) < 1e16) sb.append((long) d).append(".0");
            else sb.append(d);
        } else if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : new TreeMap<>(m).entrySet()) {
                if (!first) sb.append(", ");
                first = false;
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(": ");
                write(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object o : it) {
                if (!first) sb.append(", ");
                first = false;
                write(sb, o);
            }
            sb.append(']');
        } else {
            writeString(sb, v.toString());
        }
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    // ensure_ascii=True, Python's default.
                    if (c < 0x20 || c > 0x7e) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }
}
