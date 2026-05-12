// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.catalog.ColumnStatistics;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encodes a {@code List<ColumnStatistics>} into the IPC wire shape consumed by
 * the DuckDB extension's {@code catalog_table_column_statistics_get} and
 * {@code table_function_statistics} responses.
 *
 * <p>Wire schema (matches the Python {@code serialize_column_statistics}):
 * <pre>
 *   column_name        utf8 NOT NULL
 *   min                sparse_union&lt;child types per distinct ArrowType&gt;
 *   max                sparse_union&lt;child types per distinct ArrowType&gt;
 *   has_null           bool NOT NULL
 *   has_not_null       bool NOT NULL
 *   distinct_count     int64
 *   contains_unicode   bool
 *   max_string_length  uint64
 * </pre>
 *
 * <p>Type-code assignment is in order of first appearance, starting at 0,
 * matching the Python serializer's convention. Field names of union children
 * are the stringified type code (e.g. {@code "0"}, {@code "1"}, ...).
 */
public final class ColumnStatisticsSerializer {

    private ColumnStatisticsSerializer() {}

    public static byte[] serialize(List<ColumnStatistics> stats) {
        int n = stats.size();
        BufferAllocator alloc = Allocators.root().newChildAllocator(
                "ColumnStatisticsSerializer", 0, Long.MAX_VALUE);
        try {
            Map<ArrowType, Byte> typeMap = new LinkedHashMap<>();
            byte[] rowTypeCodes = new byte[n];
            for (int i = 0; i < n; i++) {
                ArrowType t = effectiveType(stats.get(i));
                Byte code = typeMap.get(t);
                if (code == null) {
                    code = (byte) typeMap.size();
                    typeMap.put(t, code);
                }
                rowTypeCodes[i] = code;
            }
            if (typeMap.isEmpty()) {
                typeMap.put(new ArrowType.Null(), (byte) 0);
            }

            int[] typeIds = new int[typeMap.size()];
            List<Field> unionChildren = new ArrayList<>(typeMap.size());
            int idx = 0;
            for (Map.Entry<ArrowType, Byte> e : typeMap.entrySet()) {
                typeIds[idx] = e.getValue() & 0xff;
                unionChildren.add(new Field(String.valueOf(e.getValue() & 0xff),
                        FieldType.nullable(e.getKey()), null));
                idx++;
            }
            ArrowType.Union unionType = new ArrowType.Union(UnionMode.Sparse, typeIds);

            Field minField = new Field("min",
                    new FieldType(true, unionType, null), unionChildren);
            Field maxField = new Field("max",
                    new FieldType(true, unionType, null), unionChildren);

            Schema schema = new Schema(List.of(
                    Schemas.nonNull("column_name", Schemas.UTF8),
                    minField, maxField,
                    Schemas.nonNull("has_null", Schemas.BOOL),
                    Schemas.nonNull("has_not_null", Schemas.BOOL),
                    Schemas.nullable("distinct_count", Schemas.INT64),
                    Schemas.nullable("contains_unicode", Schemas.BOOL),
                    Schemas.nullable("max_string_length", new ArrowType.Int(64, false))));

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                root.allocateNew();
                VarCharVector nameVec = (VarCharVector) root.getVector("column_name");
                BitVector hasNullVec = (BitVector) root.getVector("has_null");
                BitVector hasNotNullVec = (BitVector) root.getVector("has_not_null");
                BigIntVector distinctVec = (BigIntVector) root.getVector("distinct_count");
                BitVector unicodeVec = (BitVector) root.getVector("contains_unicode");
                UInt8Vector maxLenVec = (UInt8Vector) root.getVector("max_string_length");
                UnionVector minVec = (UnionVector) root.getVector("min");
                UnionVector maxVec = (UnionVector) root.getVector("max");

                List<FieldVector> minChildren = minVec.getChildrenFromFields();
                List<FieldVector> maxChildren = maxVec.getChildrenFromFields();
                // Sparse-union semantics: every child vector must have a valid
                // slot at every row (the child at the active typeId carries
                // the value; others are null/undefined but still allocated).
                for (FieldVector c : minChildren) c.setInitialCapacity(n);
                for (FieldVector c : maxChildren) c.setInitialCapacity(n);

                for (int i = 0; i < n; i++) {
                    ColumnStatistics s = stats.get(i);
                    nameVec.setSafe(i, s.columnName().getBytes(StandardCharsets.UTF_8));
                    hasNullVec.setSafe(i, s.hasNull() ? 1 : 0);
                    hasNotNullVec.setSafe(i, s.hasNotNull() ? 1 : 0);
                    if (s.distinctCount() != null) distinctVec.setSafe(i, s.distinctCount());
                    else distinctVec.setNull(i);
                    if (s.containsUnicode() != null) {
                        unicodeVec.setSafe(i, s.containsUnicode() ? 1 : 0);
                    } else {
                        unicodeVec.setNull(i);
                    }
                    if (s.maxStringLength() != null) maxLenVec.setSafe(i, s.maxStringLength());
                    else maxLenVec.setNull(i);

                    byte tc = rowTypeCodes[i];
                    writeUnionRow(minVec, minChildren, i, tc, s.min());
                    writeUnionRow(maxVec, maxChildren, i, tc, s.max());
                }
                nameVec.setValueCount(n);
                hasNullVec.setValueCount(n);
                hasNotNullVec.setValueCount(n);
                distinctVec.setValueCount(n);
                unicodeVec.setValueCount(n);
                maxLenVec.setValueCount(n);
                for (FieldVector c : minChildren) c.setValueCount(n);
                for (FieldVector c : maxChildren) c.setValueCount(n);
                minVec.setValueCount(n);
                maxVec.setValueCount(n);
                root.setRowCount(n);
                return BatchUtil.writeSingleBatch(root);
            }
        } finally {
            alloc.close();
        }
    }

    private static ArrowType effectiveType(ColumnStatistics s) {
        if (s.arrowType() != null) return s.arrowType();
        return new ArrowType.Null();
    }

    private static void writeUnionRow(UnionVector unionVec, List<FieldVector> children,
                                         int row, byte typeCode, Object value) {
        // Write the type code into the type buffer. UnionVector.getTypeBuffer
        // returns the underlying ArrowBuf; sparse-union type buffer is byte-
        // sized per row.
        unionVec.getTypeBuffer().setByte((long) row * UnionVector.TYPE_WIDTH, typeCode);
        if (value == null) return;
        FieldVector child = children.get(typeCode & 0xff);
        writeScalar(child, row, value);
    }

    private static void writeScalar(FieldVector v, int row, Object value) {
        if (v instanceof BigIntVector bi) {
            bi.setSafe(row, ((Number) value).longValue());
        } else if (v instanceof Float8Vector f8) {
            f8.setSafe(row, ((Number) value).doubleValue());
        } else if (v instanceof VarCharVector vc) {
            byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
            vc.setSafe(row, bytes);
        } else if (v instanceof BitVector b) {
            b.setSafe(row, ((Boolean) value) ? 1 : 0);
        } else {
            throw new IllegalArgumentException(
                    "ColumnStatisticsSerializer: unsupported stat type " + v.getClass().getSimpleName());
        }
    }
}
