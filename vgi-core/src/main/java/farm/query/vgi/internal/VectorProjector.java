// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

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
}
