// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.tableinout.TableInOutFunction;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Function overload resolution. Picks the variant whose declared
 * {@link ArgSpec}s best match the actual arguments seen at bind time.
 *
 * <p>Two-stage:
 * <ol>
 *   <li>Filter by arity — exact arity match plus varargs catch-alls
 *       whose declared arity is {@code <= argCount}.</li>
 *   <li>Score by per-position type alignment (const args use the parsed
 *       value's inferred type; column args use {@code inputSchema} at the
 *       column's position). Tie-break prefers non-varargs variants so that
 *       a concrete {@code foo(int)} outranks {@code foo(int, ...)} when
 *       both fit equally well.</li>
 * </ol>
 */
final class OverloadResolver {

    private OverloadResolver() {}

    static <T> T pick(List<T> variants, int argCount) {
        return pick(variants, argCount, null, null);
    }

    static <T> T pick(List<T> variants, int argCount, Arguments args, Schema inputSchema) {
        if (variants == null || variants.isEmpty()) return null;
        if (variants.size() == 1) return variants.get(0);

        List<T> matching = new ArrayList<>();
        for (T v : variants) {
            // Blended ("UNNEST-style"): the positional params ARE the per-row
            // input columns, absent from the wire arguments, so resolve by
            // INPUT-COLUMN count against the declared POSITIONAL arity —
            // geo_encode(52,13) -> the 2-positional overload, geo_encode(52,13,100)
            // -> the 3-positional one. Named (-1 position) specs never count
            // toward arity. Varargs matches any input-column count >= the fixed
            // positional count. Mirrors vgi-python's _match_function_arguments.
            if (v instanceof farm.query.vgi.tableinout.RowTransformFunction) {
                List<ArgSpec> specs = argSpecs(v);
                int fixed = 0;
                boolean varargs = false;
                if (specs != null) {
                    for (ArgSpec s : specs) {
                        if (s.position() < 0) continue;
                        if (s.varargs()) varargs = true;
                        else fixed++;
                    }
                }
                int inputCols = inputSchema == null ? 0 : inputSchema.getFields().size();
                if (varargs ? inputCols >= fixed : inputCols == fixed) matching.add(v);
                continue;
            }
            int n = countArgs(v);
            if (n == argCount) { matching.add(v); continue; }
            if (hasVarargs(v) && argCount >= n) matching.add(v);
        }
        if (matching.isEmpty()) matching = variants;
        if (matching.size() == 1) return matching.get(0);

        T best = matching.get(0);
        int bestScore = scoreTypeMatch(best, args, inputSchema);
        boolean bestVar = hasVarargs(best);
        for (int i = 1; i < matching.size(); i++) {
            T v = matching.get(i);
            int score = scoreTypeMatch(v, args, inputSchema);
            boolean varv = hasVarargs(v);
            if (score > bestScore || (score == bestScore && bestVar && !varv)) {
                bestScore = score; best = v; bestVar = varv;
            }
        }
        return best;
    }

    private static int countArgs(Object fn) {
        List<ArgSpec> specs = argSpecs(fn);
        return specs == null ? -1 : specs.size();
    }

    private static boolean hasVarargs(Object fn) {
        List<ArgSpec> specs = argSpecs(fn);
        if (specs == null || specs.isEmpty()) return false;
        for (ArgSpec s : specs) if (s.varargs()) return true;
        return false;
    }

    private static List<ArgSpec> argSpecs(Object fn) {
        if (fn instanceof ScalarFunction f) return f.argumentSpecs();
        if (fn instanceof TableFunction f) return f.argumentSpecs();
        if (fn instanceof TableInOutFunction f) return f.argumentSpecs();
        return null;
    }

    private static int scoreTypeMatch(Object fn, Arguments args, Schema inputSchema) {
        List<ArgSpec> specs = argSpecs(fn);
        if (specs == null) return 0;
        int constN = args == null ? 0 : args.positional().size();
        int colN = inputSchema == null ? 0 : inputSchema.getFields().size();
        int total = constN + colN;
        int score = 0;
        for (int i = 0; i < total; i++) {
            ArgSpec spec = specAt(specs, i);
            if (spec == null || spec.anyType()) continue;
            ArrowType expected = spec.arrowType();
            ArrowType actual = i < constN
                    ? inferConstType(args.positional().get(i))
                    : inputSchema.getFields().get(i - constN).getType();
            if (actual == null || expected == null) continue;
            score += typesAlign(expected, actual) ? 1 : -1;
        }
        return score;
    }

    private static ArgSpec specAt(List<ArgSpec> specs, int position) {
        for (ArgSpec s : specs) {
            if (s.position() == position) return s;
        }
        ArgSpec va = null;
        for (ArgSpec s : specs) {
            if (s.varargs() && s.position() >= 0 && s.position() <= position) va = s;
        }
        return va;
    }

    private static ArrowType inferConstType(Object v) {
        if (v == null) return null;
        if (v instanceof Boolean) return new ArrowType.Bool();
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return new ArrowType.Int(64, true);
        }
        if (v instanceof Double || v instanceof Float) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if (v instanceof CharSequence) return new ArrowType.Utf8();
        if (v instanceof byte[]) return new ArrowType.Binary();
        return null;
    }

    private static boolean typesAlign(ArrowType expected, ArrowType actual) {
        if (expected.getClass() != actual.getClass()) return false;
        if (expected instanceof ArrowType.Int e && actual instanceof ArrowType.Int a) {
            return e.getBitWidth() == a.getBitWidth() && e.getIsSigned() == a.getIsSigned();
        }
        if (expected instanceof ArrowType.FloatingPoint e && actual instanceof ArrowType.FloatingPoint a) {
            return e.getPrecision() == a.getPrecision();
        }
        return expected.equals(actual);
    }
}
