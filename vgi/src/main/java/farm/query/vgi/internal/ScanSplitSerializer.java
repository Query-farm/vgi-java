// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;

/**
 * Encodes a {@link ScanSplit} as the 1-row IPC stream that rides one entry of
 * {@code PlanResponse.splits}.
 *
 * <p>Field ORDER and nullability are wire-significant — they mirror
 * {@code vgi/protocol.py}'s {@code ScanSplit} exactly, and a client rejects a
 * plan whose schema disagrees, naming the field count.</p>
 */
public final class ScanSplitSerializer {

    private ScanSplitSerializer() {}

    private static final ArrowType BINARY = new ArrowType.Binary();
    private static final ArrowType INT64 = new ArrowType.Int(64, true);
    private static final ArrowType BOOL = new ArrowType.Bool();

    private static Field bin(String name, boolean nullable) {
        return new Field(name, new FieldType(nullable, BINARY, null), null);
    }

    private static Field i64(String name, boolean nullable) {
        return new Field(name, new FieldType(nullable, INT64, null), null);
    }

    /**
     * Serialise one split to its wire bytes.
     *
     * @param split the split, with its framework-stamped token already attached
     * @return the 1-row IPC stream bytes for this split
     */
    public static byte[] serialize(ScanSplit split) {
        BufferAllocator alloc = Allocators.root();
        Schema schema = new Schema(List.of(
                bin("payload", false),
                bin("token", false),
                i64("estimated_rows", true),
                new Field("rows_exact", new FieldType(false, BOOL, null), null),
                i64("estimated_bytes", true),
                bin("partition_bounds", true),
                bin("column_statistics", true),
                new Field("location_ids", new FieldType(true, new ArrowType.List(), null),
                        List.of(i64("item", true))),
                bin("start_position", true),
                bin("end_position", true)));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
            root.allocateNew();
            setBinary(root, "payload", split.payload() == null ? new byte[0] : split.payload());
            setBinary(root, "token", split.token() == null ? new byte[0] : split.token());
            setLong(root, "estimated_rows", split.estimatedRows());
            ((BitVector) root.getVector("rows_exact")).setSafe(0, split.rowsExact() ? 1 : 0);
            setLong(root, "estimated_bytes", split.estimatedBytes());
            setBinary(root, "partition_bounds", split.partitionBounds());
            setBinary(root, "column_statistics", split.columnStatistics());
            setBinary(root, "start_position", split.startPosition());
            setBinary(root, "end_position", split.endPosition());

            ListVector locations = (ListVector) root.getVector("location_ids");
            if (split.locationIds() == null) {
                locations.setNull(0);
            } else {
                UnionListWriter w = locations.getWriter();
                w.setPosition(0);
                w.startList();
                for (Long id : split.locationIds()) {
                    w.bigInt().writeBigInt(id == null ? 0L : id);
                }
                w.endList();
            }

            root.setRowCount(1);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer =
                    new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("serialising ScanSplit", e);
        }
    }

    private static void setBinary(VectorSchemaRoot root, String name, byte[] value) {
        if (root.getVector(name) == null) {
            return;
        }
        VarBinaryVector v = (VarBinaryVector) root.getVector(name);
        if (value == null) {
            v.setNull(0);
        } else {
            v.setSafe(0, value);
        }
    }

    private static void setLong(VectorSchemaRoot root, String name, Long value) {
        if (root.getVector(name) == null) {
            return;
        }
        BigIntVector v = (BigIntVector) root.getVector(name);
        if (value == null) {
            v.setNull(0);
        } else {
            v.setSafe(0, value);
        }
    }
}
