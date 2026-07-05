// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Bind-time enforcement of per-argument value constraints (closed choice set,
 * numeric range, and regex pattern) declared on a function's const arguments.
 *
 * <p>Shared by every function-kind bind path (scalar, table, table-in-out,
 * table-buffering, aggregate) so a declared constraint is rejected at bind
 * regardless of function type, mirroring the Python SDK's {@code Arg._validate}.
 * Column (non-const) arguments are not enforced here — that is the type-bound
 * check's domain.
 */
public final class ConstraintEnforcer {

    private ConstraintEnforcer() {}

    /**
     * Validate a function's const arguments against their declared constraints.
     *
     * <p>Const arguments are numbered sequentially (the i-th const spec reads the
     * i-th positional value); a value that violates a declared choices /
     * ge/le/gt/lt / pattern constraint throws {@link IllegalArgumentException}. A
     * null (absent) value is skipped, matching the other SDKs.
     *
     * @param args the bound call arguments (may be {@code null}).
     * @param specs the function's argument specs in declaration order (may be {@code null}).
     */
    public static void enforce(Arguments args, List<ArgSpec> specs) {
        if (args == null || specs == null) return;
        int constIdx = 0;
        for (ArgSpec spec : specs) {
            if (!spec.isConst()) continue;
            ArgSpec.Constraints c = spec.constraints();
            if (c != null && !c.isEmpty()) {
                Object value = constIdx < args.positional().size()
                        ? args.positional().get(constIdx)
                        : null;
                validateConstValue(spec.name(), c, value);
            }
            constIdx++;
        }
    }

    private static void validateConstValue(String name, ArgSpec.Constraints c, Object value) {
        if (value == null) return; // null / absent skips value constraints
        Double num = asDouble(value);
        if (num != null) {
            if (c.ge() != null && num < c.ge().doubleValue()) {
                throw constraintError(name, "must be >= " + formatBound(c.ge()));
            }
            if (c.le() != null && num > c.le().doubleValue()) {
                throw constraintError(name, "must be <= " + formatBound(c.le()));
            }
            if (c.gt() != null && num <= c.gt().doubleValue()) {
                throw constraintError(name, "must be > " + formatBound(c.gt()));
            }
            if (c.lt() != null && num >= c.lt().doubleValue()) {
                throw constraintError(name, "must be < " + formatBound(c.lt()));
            }
        }
        if (c.choices() != null && !c.choices().isEmpty() && !choiceMatches(c.choices(), value)) {
            throw constraintError(name, "must be one of " + c.choices());
        }
        if (c.pattern() != null && !c.pattern().isEmpty() && value instanceof String s) {
            try {
                if (!Pattern.matches(c.pattern(), s)) {
                    throw constraintError(name, "must match pattern " + c.pattern());
                }
            } catch (PatternSyntaxException ignored) {
                // An invalid declared regex must not reject the caller.
            }
        }
    }

    private static Double asDouble(Object value) {
        return value instanceof Number n ? n.doubleValue() : null;
    }

    private static boolean choiceMatches(List<Object> choices, Object value) {
        Double vn = asDouble(value);
        for (Object choice : choices) {
            Double cn = asDouble(choice);
            if (cn != null && vn != null) {
                if (cn.doubleValue() == vn.doubleValue()) return true;
            } else if (Objects.equals(choice, value)) {
                return true;
            }
        }
        return false;
    }

    private static String formatBound(Number n) {
        double d = n.doubleValue();
        if (Double.isFinite(d) && d == Math.rint(d)) {
            return Long.toString((long) d);
        }
        return Double.toString(d);
    }

    private static IllegalArgumentException constraintError(String name, String detail) {
        return new IllegalArgumentException("argument '" + name + "' " + detail);
    }
}
