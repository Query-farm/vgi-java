// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import java.util.ArrayList;
import java.util.List;

/**
 * npm-style version helpers used to resolve {@code IMPLEMENTATION_VERSION}
 * specs ({@code ^1.2.3}, {@code ~1.2.3}, {@code 1.2}, bare {@code 1.2.3}) and
 * to test concrete versions against ranges of the form
 * {@code ">=A.B.C,<D.E.F"}.
 *
 * <p>Versions are compared as dotted decimal ints with no pre-release / build
 * metadata support — sufficient for the catalog version matrix in the
 * integration suite.
 */
public final class SemverHelpers {

    private SemverHelpers() {}

    /** Compare two dotted-int versions. Missing components compare as zero. */
    public static int compareVersions(String a, String b) {
        String[] as = a.split("\\.");
        String[] bs = b.split("\\.");
        int n = Math.max(as.length, bs.length);
        for (int i = 0; i < n; i++) {
            int av = i < as.length ? Integer.parseInt(as[i]) : 0;
            int bv = i < bs.length ? Integer.parseInt(bs[i]) : 0;
            if (av != bv) return Integer.compare(av, bv);
        }
        return 0;
    }

    /** Test whether {@code version} satisfies a {@code ">=A.B.C,<D.E.F"}-style
     *  range. Each comma-separated clause is one of {@code >=, >, <=, <, =}. */
    public static boolean matchesRange(String version, String range) {
        String[] parts = range.split(",");
        for (String p : parts) {
            p = p.trim();
            String op;
            String val;
            if (p.startsWith(">=")) { op = ">="; val = p.substring(2); }
            else if (p.startsWith(">")) { op = ">"; val = p.substring(1); }
            else if (p.startsWith("<=")) { op = "<="; val = p.substring(2); }
            else if (p.startsWith("<")) { op = "<"; val = p.substring(1); }
            else if (p.startsWith("=")) { op = "="; val = p.substring(1); }
            else { op = "="; val = p; }
            int cmp = compareVersions(version, val);
            boolean ok = switch (op) {
                case ">=" -> cmp >= 0;
                case ">"  -> cmp >  0;
                case "<=" -> cmp <= 0;
                case "<"  -> cmp <  0;
                default  -> cmp == 0;
            };
            if (!ok) return false;
        }
        return true;
    }

    /**
     * Resolve an npm-style {@code spec} (exact / caret / tilde / partial)
     * against an ascending list of {@code supported} concrete versions.
     * Returns the highest matching supported version, or {@code null}.
     */
    public static String resolveNpmSpec(String spec, List<String> supported) {
        if (spec == null) return null;
        List<String> sorted = new ArrayList<>(supported);
        sorted.sort((a, b) -> -compareVersions(a, b));
        if (spec.startsWith("^")) {
            String base = spec.substring(1);
            String[] bs = base.split("\\.");
            int major = Integer.parseInt(bs[0]);
            String upper = (major + 1) + ".0.0";
            for (String v : sorted) {
                if (compareVersions(v, base) >= 0 && compareVersions(v, upper) < 0) return v;
            }
            return null;
        }
        if (spec.startsWith("~")) {
            String base = spec.substring(1);
            String[] bs = base.split("\\.");
            int major = Integer.parseInt(bs[0]);
            int minor = bs.length > 1 ? Integer.parseInt(bs[1]) : 0;
            String upper = major + "." + (minor + 1) + ".0";
            for (String v : sorted) {
                if (compareVersions(v, base) >= 0 && compareVersions(v, upper) < 0) return v;
            }
            return null;
        }
        String[] parts = spec.split("\\.");
        if (parts.length < 3) {
            String prefix = spec + ".";
            for (String v : sorted) {
                if (v.startsWith(prefix) || v.equals(spec)) return v;
            }
            return null;
        }
        return supported.contains(spec) ? spec : null;
    }
}
