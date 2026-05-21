// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.catalog;

import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Per-column statistics returned by {@code catalog_table_column_statistics_get}
 * or {@code table_function_statistics}.
 *
 * <p>{@code arrowType} pins the sparse-union member type for {@code min}/{@code max}.
 * When both are {@code null} (column has no usable min/max), set {@code arrowType}
 * to {@code new ArrowType.Null()}.
 *
 * <p>{@code min} and {@code max} are boxed Java values matching {@code arrowType}:
 * {@link Long} for INT64, {@link Double} for FLOAT64, {@link String} for UTF8, etc.
 */
public record ColumnStatistics(
        String columnName,
        ArrowType arrowType,
        Object min,
        Object max,
        boolean hasNull,
        boolean hasNotNull,
        Long distinctCount,
        Boolean containsUnicode,
        Long maxStringLength) {

    public static ColumnStatistics ofInt64(String name, long min, long max, boolean hasNull,
                                              Long distinctCount) {
        return new ColumnStatistics(name, new ArrowType.Int(64, true), min, max, hasNull, true,
                distinctCount, null, null);
    }

    public static ColumnStatistics ofFloat64(String name, double min, double max, boolean hasNull,
                                                Long distinctCount) {
        return new ColumnStatistics(name,
                new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
                min, max, hasNull, true, distinctCount, null, null);
    }

    public static ColumnStatistics ofUtf8(String name, String min, String max, boolean hasNull,
                                             Long distinctCount, Boolean containsUnicode,
                                             Long maxStringLength) {
        return new ColumnStatistics(name, new ArrowType.Utf8(), min, max, hasNull, true,
                distinctCount, containsUnicode, maxStringLength);
    }

    /**
     * Geometry-column stats: {@code min}/{@code max} are WKB-encoded corner-point
     * geometries (the spatial bounding box). Sent as plain {@code binary} on the
     * sparse-union wire — the C++ extension correlates {@code name} with the
     * table's {@code geoarrow.wkb}-typed column and rebuilds the spatial extent.
     */
    public static ColumnStatistics ofGeometry(String name, byte[] min, byte[] max, boolean hasNull,
                                                 Long distinctCount) {
        return new ColumnStatistics(name, new ArrowType.Binary(), min, max, hasNull, true,
                distinctCount, null, null);
    }
}
