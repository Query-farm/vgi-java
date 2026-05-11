// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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

    public interface Int64Op {
        long apply(int row);
    }

    public interface Float64Op {
        double apply(int row);
    }

    public interface StringOp {
        String apply(int row);
    }

    /**
     * Build an int64 result column by invoking {@code op} once per row of
     * {@code input}. NULL inputs yield NULL outputs (default null handling).
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

    /** Build an int64 result column unconditionally (caller handles NULLs). */
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

    /** Build a utf8 result column by invoking {@code op} once per row. */
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

    public static long toLong(FieldVector v, int row) {
        if (v instanceof BigIntVector b) return b.get(row);
        if (v instanceof IntVector i) return i.get(row);
        if (v instanceof SmallIntVector s) return s.get(row);
        if (v instanceof TinyIntVector t) return t.get(row);
        throw new ClassCastException("not an integer vector: " + v.getClass().getSimpleName());
    }

    public static double toDouble(FieldVector v, int row) {
        if (v instanceof Float8Vector f) return f.get(row);
        if (v instanceof Float4Vector f) return f.get(row);
        if (v instanceof BigIntVector b) return b.get(row);
        if (v instanceof IntVector i) return i.get(row);
        if (v instanceof SmallIntVector s) return s.get(row);
        if (v instanceof TinyIntVector t) return t.get(row);
        if (v instanceof org.apache.arrow.vector.DecimalVector d) {
            int scale = ((org.apache.arrow.vector.types.pojo.ArrowType.Decimal)
                    d.getField().getType()).getScale();
            java.math.BigDecimal bd = d.getObject(row);
            return bd == null ? 0.0 : bd.doubleValue();
        }
        if (v instanceof org.apache.arrow.vector.Decimal256Vector d) {
            java.math.BigDecimal bd = d.getObject(row);
            return bd == null ? 0.0 : bd.doubleValue();
        }
        throw new ClassCastException("not a numeric vector: " + v.getClass().getSimpleName());
    }

    public static String toString(FieldVector v, int row) {
        if (v instanceof VarCharVector s) {
            byte[] bytes = s.get(row);
            return bytes == null ? null : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new ClassCastException("not a varchar vector: " + v.getClass().getSimpleName());
    }

    public static boolean toBool(FieldVector v, int row) {
        if (v instanceof BitVector b) return b.get(row) != 0;
        throw new ClassCastException("not a bool vector: " + v.getClass().getSimpleName());
    }
}
