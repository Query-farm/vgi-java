// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

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

    /**
     * Project {@code src} down to {@code target}'s columns by name, copying row-by-row.
     *
     * @param src the source root; closed unless returned unchanged
     * @param target the desired (subset) schema
     * @return {@code src} when its schema already equals {@code target}, otherwise a fresh root holding the projected columns
     */
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
     * Move {@code src}'s buffers into a fresh, caller-owned vector typed as {@code field}.
     *
     * <p>Arrow's own {@code getTransferPair} overloads each lose field metadata
     * for some vector type, and with it the {@code ARROW:extension:*} tag that
     * DuckDB reads to tell a UUID ({@code arrow.uuid}) or HUGEINT
     * ({@code arrow.opaque}) apart from its storage type: the
     * allocator overload rebuilds a {@code FixedSizeBinaryVector} from its
     * name and width alone, and the {@code Field} overload drops the metadata
     * of a struct's children. An emitted column that has lost its tag no
     * longer matches the type declared at bind, which the extension refuses.
     * Building the target from a whole {@code Field} and transferring into it
     * keeps every level.</p>
     *
     * <p>That {@code Field} takes its types from {@code src} and its names,
     * nullability and metadata from {@code field}. A schema describes a
     * dictionary-encoded column by its value type ({@code Utf8} for a DuckDB ENUM) while the vector holds
     * the indices ({@code UInt1}), so a target built from the schema alone would
     * be the wrong vector.</p>
     *
     * @param src the vector to empty; left valid but holding no buffers
     * @param field the names and metadata to give the target, normally the schema's rather than {@code src.getField()}
     * @return a new vector owning {@code src}'s former buffers
     */
    public static FieldVector transfer(FieldVector src, Field field) {
        FieldVector dst = labelled(src.getField(), field).createVector(Allocators.root());
        src.makeTransferPair(dst).transfer();
        return dst;
    }

    /** {@code shape}'s types and dictionaries under {@code labels}' names, nullability and metadata, level by level. */
    private static Field labelled(Field shape, Field labels) {
        List<Field> shapeChildren = shape.getChildren();
        List<Field> labelChildren = labels.getChildren();
        List<Field> children = shapeChildren;
        if (shapeChildren.size() == labelChildren.size() && !shapeChildren.isEmpty()) {
            children = new ArrayList<>(shapeChildren.size());
            for (int i = 0; i < shapeChildren.size(); i++) {
                children.add(labelled(shapeChildren.get(i), labelChildren.get(i)));
            }
        }
        return new Field(labels.getName(),
                new FieldType(labels.isNullable(), shape.getType(), shape.getDictionary(), labels.getMetadata()),
                children);
    }

    /**
     * Move {@code src}'s columns into a fresh root the caller owns, keeping every
     * field's metadata (see {@link #transfer(FieldVector, Field)}). Use it to emit
     * an input batch: the reader owns and reuses {@code src}, and the framework
     * closes each emitted root.
     *
     * <p>Metadata comes from a schema, never from the vectors: a transport that
     * detached the batch before the function saw it may have rebuilt the vectors
     * with the same Arrow flaw, while keeping the root's schema intact.</p>
     *
     * @param src the source root; its vectors are emptied, the root itself stays open
     * @param target the columns to take, by name, and the labels to give them;
     *               {@code null} takes every column under {@code src}'s own schema
     * @return a fresh root holding {@code src}'s former buffers
     */
    public static VectorSchemaRoot detach(VectorSchemaRoot src, Schema target) {
        List<Field> fields = (target == null ? src.getSchema() : target).getFields();
        List<FieldVector> moved = new ArrayList<>(fields.size());
        for (Field f : fields) {
            FieldVector sv = src.getVector(f.getName());
            if (sv == null) {
                moved.forEach(FieldVector::close);
                throw new IllegalArgumentException(
                        "column '" + f.getName() + "' missing from batch " + src.getSchema());
            }
            moved.add(transfer(sv, f));
        }
        VectorSchemaRoot dst = new VectorSchemaRoot(moved);
        dst.setRowCount(src.getRowCount());
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
     *
     * @param src the source root; closed unless returned unchanged
     * @param target the schema whose names / metadata to expose (may be {@code null} for a no-op)
     * @return a root sharing {@code src}'s buffers relabelled to {@code target}, or the result of {@link #project} when shapes don't match
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
            moved.add(transfer(src.getVector(i), dstFields.get(i)));
        }
        src.close();
        VectorSchemaRoot dst = new VectorSchemaRoot(moved);
        dst.setRowCount(rows);
        return dst;
    }
}
