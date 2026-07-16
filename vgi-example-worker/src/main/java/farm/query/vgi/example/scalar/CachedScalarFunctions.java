// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;

import java.nio.charset.StandardCharsets;

/**
 * Cacheable scalar fixtures backing the extension's per-value memoization
 * ({@code scalar/per_value*.test}, {@code cache/per_value_*.test}). Each
 * advertises {@code vgi.cache.*} via {@link farm.query.vgi.scalar.ScalarFunction#cacheControl()},
 * so the C++ side memoizes the output per distinct input value. Mirrors
 * vgi-python's {@code CachedDoubleScalarFunction} /
 * {@code CachedAddConstScalarFunction} / {@code CachedLabelScalarFunction}.
 */
public final class CachedScalarFunctions {

    private CachedScalarFunctions() {}

    /**
     * {@code cached_double_scalar(value INT64) -> INT64} — a deterministic 1:1
     * double advertising {@code vgi.cache.ttl}, so opting into the result cache
     * is sound: a fully-warm distinct set is served without the worker.
     */
    public static final class CachedDoubleScalarFunction extends ScalarFn {

        @Override public String name() { return "cached_double_scalar"; }
        @Override public String description() {
            return "Doubles a BIGINT value (advertises vgi.cache.ttl for per-value memo)";
        }
        @Override public CacheControl cacheControl() { return CacheControl.ttl(300); }

        /**
         * Double each input value; nulls propagate.
         *
         * @param value the values to double.
         * @param result the output vector (framework-allocated).
         */
        public void compute(@Vector(doc = "Value to double") BigIntVector value,
                             BigIntVector result) {
            int rows = value.getValueCount();
            for (int i = 0; i < rows; i++) {
                if (value.isNull(i)) result.setNull(i);
                else result.setSafe(i, value.get(i) * 2);
            }
            result.setValueCount(rows);
        }
    }

    /**
     * {@code cached_add_const(value INT64, addend INT64 [const]) -> INT64} —
     * proves the const arg is folded into the per-value cache key: two calls
     * with the same {@code value} but different {@code addend} must not
     * cross-serve.
     */
    public static final class CachedAddConstScalarFunction extends ScalarFn {

        @Override public String name() { return "cached_add_const"; }
        @Override public String description() {
            return "value + const addend (advertises vgi.cache.ttl)";
        }
        @Override public CacheControl cacheControl() { return CacheControl.ttl(300); }

        /**
         * Add the const addend to each value; nulls propagate.
         *
         * @param value the values to add to.
         * @param addend the constant addend.
         * @param result the output vector (framework-allocated).
         */
        public void compute(@Vector(doc = "Value") BigIntVector value,
                             @Const(doc = "Constant addend") long addend,
                             BigIntVector result) {
            int rows = value.getValueCount();
            for (int i = 0; i < rows; i++) {
                if (value.isNull(i)) result.setNull(i);
                else result.setSafe(i, value.get(i) + addend);
            }
            result.setValueCount(rows);
        }
    }

    /**
     * {@code cached_label(value INT64) -> VARCHAR} — returns
     * {@code lbl-<value>} for {@code value >= 0}, NULL otherwise, so a cached
     * per-value entry must round-trip both a heap string and a null.
     */
    public static final class CachedLabelScalarFunction extends ScalarFn {

        @Override public String name() { return "cached_label"; }
        @Override public String description() {
            return "value -> 'lbl-<value>' or NULL for negatives (advertises vgi.cache.ttl)";
        }
        @Override public CacheControl cacheControl() { return CacheControl.ttl(300); }

        /**
         * Label non-negative values; NULL for negatives and null inputs.
         *
         * @param value the values to label.
         * @param result the output vector (framework-allocated).
         */
        public void compute(@Vector(doc = "Value") BigIntVector value,
                             VarCharVector result) {
            int rows = value.getValueCount();
            for (int i = 0; i < rows; i++) {
                if (value.isNull(i) || value.get(i) < 0) {
                    result.setNull(i);
                    continue;
                }
                byte[] label = ("lbl-" + value.get(i)).getBytes(StandardCharsets.UTF_8);
                result.setSafe(i, label, 0, label.length);
            }
            result.setValueCount(rows);
        }
    }
}
