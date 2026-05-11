// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/**
 * Declares the 20 ATTACH options used by
 * {@code test/sql/integration/attach/attach_options_echo.test}: one option per
 * supported Arrow/DuckDB type, each carrying a representative default.
 */
public final class AttachOptionsFixture {

    private AttachOptionsFixture() {}

    public static final String CATALOG_NAME = "attach_options";

    private static final ArrowType DATE32 = new ArrowType.Date(DateUnit.DAY);
    private static final ArrowType TIME64_US = new ArrowType.Time(TimeUnit.MICROSECOND, 64);
    private static final ArrowType TIMESTAMP_US = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    private static final ArrowType TIMESTAMP_US_UTC = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    private static final ArrowType DECIMAL_18_4 = new ArrowType.Decimal(18, 4, 128);
    private static final ArrowType LIST = new ArrowType.List();
    private static final ArrowType STRUCT = new ArrowType.Struct();
    private static final ArrowType INT8 = new ArrowType.Int(8, true);
    private static final ArrowType INT16 = new ArrowType.Int(16, true);
    private static final ArrowType INT32 = new ArrowType.Int(32, true);
    private static final ArrowType UINT8 = new ArrowType.Int(8, false);
    private static final ArrowType UINT16 = new ArrowType.Int(16, false);
    private static final ArrowType UINT32 = new ArrowType.Int(32, false);
    private static final ArrowType UINT64 = new ArrowType.Int(64, false);
    private static final ArrowType FLOAT32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

    public static List<AttachOptionSpec> declaredSpecs() {
        int defaultDateDays = (int) LocalDate.of(2026, 4, 24).toEpochDay();
        long defaultTimeUs = LocalTime.of(12, 34, 56).toNanoOfDay() / 1_000L;
        long defaultTsUs = LocalDateTime.of(2026, 4, 24, 12, 34, 56)
                .toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
        return List.of(
                AttachOptionSpec.of("opt_bool", "Boolean option", Schemas.BOOL, Boolean.TRUE),
                AttachOptionSpec.of("opt_int8", "int8", INT8, (byte) -8),
                AttachOptionSpec.of("opt_int16", "int16", INT16, (short) -16),
                AttachOptionSpec.of("opt_int32", "int32", INT32, -32),
                AttachOptionSpec.of("opt_int64", "int64", Schemas.INT64, -64L),
                AttachOptionSpec.of("opt_uint8", "uint8", UINT8, (byte) 8),
                AttachOptionSpec.of("opt_uint16", "uint16", UINT16, 16),
                AttachOptionSpec.of("opt_uint32", "uint32", UINT32, 32),
                AttachOptionSpec.of("opt_uint64", "uint64", UINT64, 64L),
                AttachOptionSpec.of("opt_float32", "float32", FLOAT32, 1.5f),
                AttachOptionSpec.of("opt_float64", "float64", Schemas.FLOAT64, 2.5d),
                AttachOptionSpec.of("opt_string", "UTF-8 string", Schemas.UTF8, "hello"),
                AttachOptionSpec.of("opt_blob", "Binary blob", new ArrowType.Binary(),
                        new byte[] {0x00, 0x01, 0x02}),
                AttachOptionSpec.of("opt_date", "Date", DATE32, defaultDateDays),
                AttachOptionSpec.of("opt_time", "Time of day", TIME64_US, defaultTimeUs),
                AttachOptionSpec.of("opt_timestamp", "Naive timestamp", TIMESTAMP_US, defaultTsUs),
                AttachOptionSpec.of("opt_timestamp_tz", "Timestamp with UTC tz",
                        TIMESTAMP_US_UTC, defaultTsUs),
                AttachOptionSpec.of("opt_decimal", "Decimal(18,4)", DECIMAL_18_4,
                        new BigDecimal("123.4500")),
                AttachOptionSpec.of("opt_list", "List of int64", LIST,
                        List.of(new Field("item",
                                new FieldType(true, Schemas.INT64, null), null)),
                        List.of(1L, 2L, 3L)),
                AttachOptionSpec.of("opt_struct", "Struct", STRUCT,
                        List.of(
                                new Field("a", new FieldType(true, Schemas.INT64, null), null),
                                new Field("b", new FieldType(true, Schemas.UTF8, null), null)),
                        Map.of("a", 1L, "b", "x")));
    }
}
