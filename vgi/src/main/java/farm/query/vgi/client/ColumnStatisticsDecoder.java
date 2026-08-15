// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.catalog.ColumnStatistics;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Decode the per-column statistics a worker returns from
 * {@code table_function_statistics} (and
 * {@code catalog_table_column_statistics_get}).
 *
 * <p>The inverse of the worker-side {@code ColumnStatisticsSerializer}: one row
 * per column, with {@code min} / {@code max} carried in a <em>sparse union</em>
 * so a batch can mix an int64 column's bounds with a utf8 column's. Each row's
 * active union member names the statistic's Arrow type, which is what
 * {@link ColumnStatistics#arrowType()} reports back.
 *
 * <p>An empty reply is the worker saying "no statistics" — DuckDB treats that
 * as unknown rather than as an error — so this decoder answers an empty or
 * {@code null} blob with an empty list rather than throwing.
 */
public final class ColumnStatisticsDecoder {

    private ColumnStatisticsDecoder() {}

    /**
     * Decode a statistics reply.
     *
     * @param data the IPC bytes from {@code table_function_statistics}, or
     *             {@code null}/empty for "no statistics"
     * @return one entry per column, in the worker's order; empty when there are none
     * @throws IllegalStateException if the bytes are present but malformed
     */
    public static List<ColumnStatistics> decode(byte[] data) {
        if (data == null || data.length == 0) return List.of();
        try (ByteArrayInputStream in = new ByteArrayInputStream(data);
             ArrowStreamReader reader = new ArrowStreamReader(in, Allocators.root())) {
            if (!reader.loadNextBatch()) return List.of();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            FieldVector name = root.getVector("column_name");
            UnionVector min = (UnionVector) root.getVector("min");
            UnionVector max = (UnionVector) root.getVector("max");
            FieldVector hasNull = root.getVector("has_null");
            FieldVector hasNotNull = root.getVector("has_not_null");
            FieldVector distinct = root.getVector("distinct_count");
            FieldVector unicode = root.getVector("contains_unicode");
            FieldVector maxLen = root.getVector("max_string_length");

            List<ColumnStatistics> out = new ArrayList<>(root.getRowCount());
            for (int row = 0; row < root.getRowCount(); row++) {
                out.add(new ColumnStatistics(
                        (String) VectorScalarCodec.read(name, row),
                        unionType(min, row),
                        unionValue(min, row),
                        unionValue(max, row),
                        Boolean.TRUE.equals(VectorScalarCodec.read(hasNull, row)),
                        Boolean.TRUE.equals(VectorScalarCodec.read(hasNotNull, row)),
                        (Long) VectorScalarCodec.read(distinct, row),
                        (Boolean) VectorScalarCodec.read(unicode, row),
                        (Long) VectorScalarCodec.read(maxLen, row)));
            }
            return List.copyOf(out);
        } catch (Exception e) {
            throw new IllegalStateException("ColumnStatisticsDecoder.decode failed", e);
        }
    }

    /**
     * The Arrow type of a row's active union member — {@code ArrowType.Null}
     * when the worker reported no usable bounds for the column.
     */
    private static ArrowType unionType(UnionVector union, int row) {
        FieldVector child = activeChild(union, row);
        return child == null ? new ArrowType.Null() : child.getField().getType();
    }

    private static Object unionValue(UnionVector union, int row) {
        FieldVector child = activeChild(union, row);
        return child == null ? null : VectorScalarCodec.read(child, row);
    }

    /**
     * Sparse-union member lookup: the row's type id selects the child, and the
     * value lives at the <em>same</em> row index inside it (that is what makes
     * the union sparse).
     */
    private static FieldVector activeChild(UnionVector union, int row) {
        if (union == null) return null;
        return (FieldVector) union.getVectorByType(union.getTypeValue(row));
    }
}
