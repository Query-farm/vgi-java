// Copyright 2026 Query Farm LLC - https://query.farm

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
 *
 * @param columnName      name of the column these statistics describe
 * @param arrowType       Arrow type pinning the sparse-union member used for
 *                        {@code min}/{@code max}; {@code ArrowType.Null} when both
 *                        are {@code null}
 * @param min             minimum value, boxed to match {@code arrowType}, or {@code null}
 * @param max             maximum value, boxed to match {@code arrowType}, or {@code null}
 * @param hasNull         whether the column contains at least one NULL
 * @param hasNotNull      whether the column contains at least one non-NULL value
 * @param distinctCount   approximate distinct-value count, or {@code null} if unknown
 * @param containsUnicode whether a string column contains non-ASCII data, or {@code null}
 * @param maxStringLength maximum string length for a string column, or {@code null}
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

    /**
     * Statistics for an INT64 column.
     *
     * @param name          column name
     * @param min           minimum value
     * @param max           maximum value
     * @param hasNull       whether the column contains NULLs
     * @param distinctCount approximate distinct-value count, or {@code null}
     * @return the column statistics
     */
    public static ColumnStatistics ofInt64(String name, long min, long max, boolean hasNull,
                                              Long distinctCount) {
        return new ColumnStatistics(name, new ArrowType.Int(64, true), min, max, hasNull, true,
                distinctCount, null, null);
    }

    /**
     * Statistics for a FLOAT64 column.
     *
     * @param name          column name
     * @param min           minimum value
     * @param max           maximum value
     * @param hasNull       whether the column contains NULLs
     * @param distinctCount approximate distinct-value count, or {@code null}
     * @return the column statistics
     */
    public static ColumnStatistics ofFloat64(String name, double min, double max, boolean hasNull,
                                                Long distinctCount) {
        return new ColumnStatistics(name,
                new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
                min, max, hasNull, true, distinctCount, null, null);
    }

    /**
     * Statistics for a UTF8 string column.
     *
     * @param name            column name
     * @param min             minimum value
     * @param max             maximum value
     * @param hasNull         whether the column contains NULLs
     * @param distinctCount   approximate distinct-value count, or {@code null}
     * @param containsUnicode whether the column contains non-ASCII data, or {@code null}
     * @param maxStringLength maximum string length, or {@code null}
     * @return the column statistics
     */
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
     *
     * @param name          column name
     * @param min           WKB-encoded minimum corner point
     * @param max           WKB-encoded maximum corner point
     * @param hasNull       whether the column contains NULLs
     * @param distinctCount approximate distinct-value count, or {@code null}
     * @return the column statistics
     */
    public static ColumnStatistics ofGeometry(String name, byte[] min, byte[] max, boolean hasNull,
                                                 Long distinctCount) {
        return new ColumnStatistics(name, new ArrowType.Binary(), min, max, hasNull, true,
                distinctCount, null, null);
    }
}
