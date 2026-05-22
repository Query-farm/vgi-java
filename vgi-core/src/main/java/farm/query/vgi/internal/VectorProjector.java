// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;

/**
 * Column-by-name projection from a full {@link VectorSchemaRoot} down to
 * the schema DuckDB asked for (projection_pushdown). When the source already
 * matches the target schema, returns it unchanged; otherwise allocates a
 * fresh root, copies each named column row-by-row, and closes the source.
 *
 * <p>Fixtures that opt into projection pushdown call this to honour the
 * requested column subset when emitting batches.</p>
 */
public final class VectorProjector {

    private VectorProjector() {}

    public static VectorSchemaRoot project(VectorSchemaRoot src, Schema target) {
        if (src.getSchema().equals(target)) return src;
        VectorSchemaRoot dst = VectorSchemaRoot.create(target, Allocators.root());
        dst.allocateNew();
        int rows = src.getRowCount();
        for (Field f : target.getFields()) {
            FieldVector dv = dst.getVector(f.getName());
            FieldVector sv = src.getVector(f.getName());
            if (sv == null) continue;
            for (int i = 0; i < rows; i++) dv.copyFromSafe(i, i, sv);
        }
        dst.setRowCount(rows);
        src.close();
        return dst;
    }

    /**
     * Zero-copy field rename: produce a {@link VectorSchemaRoot} that exposes
     * {@code src}'s buffers under {@code target}'s field names / metadata.
     * Requires {@code src} and {@code target} to have the same column count
     * in matching positions; only the {@link Field#getName} or per-field
     * metadata may differ. Falls back to {@link #project} when shapes don't
     * match.
     *
     * <p>Use when an upstream batch source (JDBC-Arrow, Parquet) yields a
     * schema that differs from the declared output schema only by labels —
     * the row-by-row {@code copyFromSafe} loop in {@link #project} is pure
     * overhead in that case.
     */
    public static VectorSchemaRoot relabel(VectorSchemaRoot src, Schema target) {
        if (target == null || src.getSchema().equals(target)) return src;
        List<Field> srcFields = src.getSchema().getFields();
        List<Field> dstFields = target.getFields();
        if (srcFields.size() != dstFields.size()) return project(src, target);
        for (int i = 0; i < srcFields.size(); i++) {
            if (!srcFields.get(i).getType().equals(dstFields.get(i).getType())) {
                return project(src, target);
            }
        }
        int rows = src.getRowCount();
        List<FieldVector> moved = new ArrayList<>(dstFields.size());
        for (int i = 0; i < dstFields.size(); i++) {
            FieldVector sv = src.getVector(i);
            TransferPair tp = sv.getTransferPair(dstFields.get(i), Allocators.root());
            tp.transfer();
            moved.add((FieldVector) tp.getTo());
        }
        src.close();
        VectorSchemaRoot dst = new VectorSchemaRoot(moved);
        dst.setRowCount(rows);
        return dst;
    }
}
