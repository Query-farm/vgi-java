// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.types;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;


/**
 * Helpers for scalar function processing: numeric dispatch + per-row mapping.
 *
 * <p>Phase 2 supports int64 and float64 dispatch with input-side widening from
 * smaller integer / float types. Wider numeric coverage (decimal, uint*, etc.)
 * arrives in later phases.
 */
public final class ScalarHelpers {

    private ScalarHelpers() {}

    /** Per-row computation producing an {@code int64} value. */
    public interface Int64Op {
        /**
         * Compute the result for one input row.
         *
         * @param row zero-based row index
         * @return the computed {@code long} value
         */
        long apply(int row);
    }

    /** Per-row computation producing a {@code float64} value. */
    public interface Float64Op {
        /**
         * Compute the result for one input row.
         *
         * @param row zero-based row index
         * @return the computed {@code double} value
         */
        double apply(int row);
    }

    /** Per-row computation producing a {@code utf8} value (may be {@code null}). */
    public interface StringOp {
        /**
         * Compute the result for one input row.
         *
         * @param row zero-based row index
         * @return the computed string, or {@code null} for a NULL output
         */
        String apply(int row);
    }

    /**
     * Build an int64 result column by invoking {@code op} once per row of
     * {@code input}. NULL inputs yield NULL outputs (default null handling).
     *
     * @param outputSchema schema of the single-column {@code result} output
     * @param input source batch supplying the row count
     * @param alloc allocator for the new {@link VectorSchemaRoot}
     * @param nullSource optional vector whose nulls propagate to the output; may be {@code null}
     * @param op per-row computation
     * @return a freshly allocated output root, owned by the caller
     */
    public static VectorSchemaRoot mapInt64(Schema outputSchema, VectorSchemaRoot input,
                                             BufferAllocator alloc, FieldVector nullSource,
                                             Int64Op op) {
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(outputSchema, alloc);
        out.allocateNew();
        BigIntVector v = (BigIntVector) out.getVector("result");
        for (int i = 0; i < rows; i++) {
            if (nullSource != null && nullSource.isNull(i)) {
                v.setNull(i);
            } else {
                v.setSafe(i, op.apply(i));
            }
        }
        out.setRowCount(rows);
        return out;
    }

    /**
     * Build a single {@code result} column of int64 or float64 (chosen by
     * {@code outSchema}'s result field type), running {@code longOp} or
     * {@code doubleOp} per row. Rows where any of {@code inputCols} is null
     * yield null output (default null handling).
     *
     * <p>The two ops are passed as separate lambdas because the per-row
     * computation differs by output type (int sums of int inputs, double
     * sums when any input widens).</p>
     *
     * @param outSchema single-column {@code result} schema selecting int64 vs float64
     * @param alloc allocator for the new {@link VectorSchemaRoot}
     * @param inputCols input columns checked for per-row nullness
     * @param rows output row count
     * @param longOp per-row computation when the output is int64
     * @param doubleOp per-row computation when the output is float64
     * @return a freshly allocated output root, owned by the caller
     */
    public static VectorSchemaRoot mapNumericRows(Schema outSchema, BufferAllocator alloc,
                                                    List<FieldVector> inputCols, int rows,
                                                    IntToLongFunction longOp,
                                                    IntToDoubleFunction doubleOp) {
        ArrowType outType = outSchema.getFields().get(0).getType();
        boolean floating = outType instanceof ArrowType.FloatingPoint;
        VectorSchemaRoot out = VectorSchemaRoot.create(outSchema, alloc);
        out.allocateNew();
        FieldVector v = out.getVector("result");
        for (int i = 0; i < rows; i++) {
            boolean anyNull = false;
            for (FieldVector c : inputCols) {
                if (c.isNull(i)) { anyNull = true; break; }
            }
            if (anyNull) {
                v.setNull(i);
            } else if (floating) {
                ((Float8Vector) v).setSafe(i, doubleOp.applyAsDouble(i));
            } else {
                ((BigIntVector) v).setSafe(i, longOp.applyAsLong(i));
            }
        }
        out.setRowCount(rows);
        return out;
    }

    /**
     * Build an int64 result column unconditionally (caller handles NULLs).
     *
     * @param outputSchema single-column {@code result} schema
     * @param input source batch supplying the row count
     * @param alloc allocator for the new {@link VectorSchemaRoot}
     * @param op per-row computation invoked for every row
     * @return a freshly allocated output root, owned by the caller
     */
    public static VectorSchemaRoot mapInt64Raw(Schema outputSchema, VectorSchemaRoot input,
                                                BufferAllocator alloc, Int64Op op) {
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(outputSchema, alloc);
        out.allocateNew();
        BigIntVector v = (BigIntVector) out.getVector("result");
        for (int i = 0; i < rows; i++) v.setSafe(i, op.apply(i));
        out.setRowCount(rows);
        return out;
    }

    /**
     * Build a utf8 result column by invoking {@code op} once per row.
     *
     * @param outputSchema single-column {@code result} schema
     * @param input source batch supplying the row count
     * @param alloc allocator for the new {@link VectorSchemaRoot}
     * @param nullSource optional vector whose nulls propagate to the output; may be {@code null}
     * @param op per-row computation (a {@code null} return yields a NULL output cell)
     * @return a freshly allocated output root, owned by the caller
     */
    public static VectorSchemaRoot mapString(Schema outputSchema, VectorSchemaRoot input,
                                              BufferAllocator alloc, FieldVector nullSource,
                                              StringOp op) {
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(outputSchema, alloc);
        out.allocateNew();
        VarCharVector v = (VarCharVector) out.getVector("result");
        for (int i = 0; i < rows; i++) {
            if (nullSource != null && nullSource.isNull(i)) {
                v.setNull(i);
            } else {
                String s = op.apply(i);
                if (s == null) v.setNull(i);
                else v.setSafe(i, new Text(s));
            }
        }
        out.setRowCount(rows);
        return out;
    }

    // ------------------------------------------------------------------
    // Typed value extractors with widening
    // ------------------------------------------------------------------

    /**
     * Read a row as a {@code long}, widening any signed integer vector.
     *
     * @param v an integer-typed vector (TINYINT through BIGINT)
     * @param row zero-based row index
     * @return the value widened to {@code long}
     * @throws ClassCastException if {@code v} is not an integer vector
     */
    public static long toLong(FieldVector v, int row) {
        if (v instanceof BigIntVector b) return b.get(row);
        if (v instanceof IntVector i) return i.get(row);
        if (v instanceof SmallIntVector s) return s.get(row);
        if (v instanceof TinyIntVector t) return t.get(row);
        if (v instanceof org.apache.arrow.vector.UInt8Vector u) return u.get(row);
        if (v instanceof org.apache.arrow.vector.UInt4Vector u) return u.get(row) & 0xFFFFFFFFL;
        if (v instanceof org.apache.arrow.vector.UInt2Vector u) return u.get(row);
        if (v instanceof org.apache.arrow.vector.UInt1Vector u) return u.get(row) & 0xFFL;
        throw new ClassCastException("not an integer vector: " + v.getClass().getSimpleName());
    }

    /**
     * Read a row as a {@code double}, widening any numeric vector (integer,
     * float, or decimal). NULL decimal cells read as {@code 0.0}.
     *
     * @param v a numeric-typed vector
     * @param row zero-based row index
     * @return the value as a {@code double}
     * @throws ClassCastException if {@code v} is not a numeric vector
     */
    public static double toDouble(FieldVector v, int row) {
        if (v instanceof Float8Vector f) return f.get(row);
        if (v instanceof Float4Vector f) return f.get(row);
        if (v instanceof BigIntVector b) return b.get(row);
        if (v instanceof IntVector i) return i.get(row);
        if (v instanceof SmallIntVector s) return s.get(row);
        if (v instanceof TinyIntVector t) return t.get(row);
        if (v instanceof org.apache.arrow.vector.UInt8Vector u) return u.get(row);
        if (v instanceof org.apache.arrow.vector.UInt4Vector u) return u.get(row) & 0xFFFFFFFFL;
        if (v instanceof org.apache.arrow.vector.UInt2Vector u) return u.get(row);
        if (v instanceof org.apache.arrow.vector.UInt1Vector u) return u.get(row) & 0xFFL;
        if (v instanceof org.apache.arrow.vector.DecimalVector d) {
            java.math.BigDecimal bd = d.getObject(row);
            return bd == null ? 0.0 : bd.doubleValue();
        }
        if (v instanceof org.apache.arrow.vector.Decimal256Vector d) {
            java.math.BigDecimal bd = d.getObject(row);
            return bd == null ? 0.0 : bd.doubleValue();
        }
        throw new ClassCastException("not a numeric vector: " + v.getClass().getSimpleName());
    }

    /**
     * Read a row as a {@link java.math.BigDecimal}, widening any numeric vector.
     * Decimal vectors are read losslessly; integers and floats are coerced.
     *
     * @param v a numeric-typed vector
     * @param row zero-based row index
     * @return the value as a {@link java.math.BigDecimal} (never {@code null} for a non-null cell)
     * @throws ClassCastException if {@code v} is not a numeric vector
     */
    public static java.math.BigDecimal toBigDecimal(FieldVector v, int row) {
        if (v instanceof org.apache.arrow.vector.DecimalVector d) return d.getObject(row);
        if (v instanceof org.apache.arrow.vector.Decimal256Vector d) return d.getObject(row);
        if (v instanceof Float8Vector || v instanceof Float4Vector) {
            return java.math.BigDecimal.valueOf(toDouble(v, row));
        }
        return java.math.BigDecimal.valueOf(toLong(v, row));
    }

    /**
     * Read a row as a UTF-8 {@link String}.
     *
     * @param v a {@link VarCharVector}
     * @param row zero-based row index
     * @return the decoded string, or {@code null} for a NULL cell
     * @throws ClassCastException if {@code v} is not a varchar vector
     */
    public static String toString(FieldVector v, int row) {
        if (v instanceof VarCharVector s) {
            byte[] bytes = s.get(row);
            return bytes == null ? null : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new ClassCastException("not a varchar vector: " + v.getClass().getSimpleName());
    }

    /**
     * Read a row as a {@code boolean}.
     *
     * @param v a {@link BitVector}
     * @param row zero-based row index
     * @return the boolean value
     * @throws ClassCastException if {@code v} is not a bool vector
     */
    public static boolean toBool(FieldVector v, int row) {
        if (v instanceof BitVector b) return b.get(row) != 0;
        throw new ClassCastException("not a bool vector: " + v.getClass().getSimpleName());
    }
}
