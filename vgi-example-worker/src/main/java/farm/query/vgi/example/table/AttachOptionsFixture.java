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
        // Days-since-epoch for 2026-04-24.
        int defaultDateDays = (int) LocalDate.of(2026, 4, 24).toEpochDay();
        // Microseconds since midnight for 12:34:56.
        long defaultTimeUs = LocalTime.of(12, 34, 56).toNanoOfDay() / 1_000L;
        long defaultTsUs = LocalDateTime.of(2026, 4, 24, 12, 34, 56)
                .toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
        return List.of(
                new AttachOptionSpec("opt_bool", "Boolean option", Schemas.BOOL, Boolean.TRUE),
                new AttachOptionSpec("opt_int8", "int8", INT8, (byte) -8),
                new AttachOptionSpec("opt_int16", "int16", INT16, (short) -16),
                new AttachOptionSpec("opt_int32", "int32", INT32, -32),
                new AttachOptionSpec("opt_int64", "int64", Schemas.INT64, -64L),
                new AttachOptionSpec("opt_uint8", "uint8", UINT8, (byte) 8),
                new AttachOptionSpec("opt_uint16", "uint16", UINT16, 16),
                new AttachOptionSpec("opt_uint32", "uint32", UINT32, 32),
                new AttachOptionSpec("opt_uint64", "uint64", UINT64, 64L),
                new AttachOptionSpec("opt_float32", "float32", FLOAT32, 1.5f),
                new AttachOptionSpec("opt_float64", "float64", Schemas.FLOAT64, 2.5d),
                new AttachOptionSpec("opt_string", "UTF-8 string", Schemas.UTF8, "hello"),
                new AttachOptionSpec("opt_blob", "Binary blob", new ArrowType.Binary(),
                        new byte[] {0x00, 0x01, 0x02}),
                new AttachOptionSpec("opt_date", "Date", DATE32, defaultDateDays),
                new AttachOptionSpec("opt_time", "Time of day", TIME64_US, defaultTimeUs),
                new AttachOptionSpec("opt_timestamp", "Naive timestamp", TIMESTAMP_US, defaultTsUs),
                new AttachOptionSpec("opt_timestamp_tz", "Timestamp with UTC tz",
                        TIMESTAMP_US_UTC, defaultTsUs),
                new AttachOptionSpec("opt_decimal", "Decimal(18,4)", DECIMAL_18_4,
                        new BigDecimal("123.4500")),
                new AttachOptionSpec("opt_list", "List of int64", LIST,
                        List.of(new Field("item",
                                new FieldType(true, Schemas.INT64, null), null)),
                        List.of(1L, 2L, 3L)),
                new AttachOptionSpec("opt_struct", "Struct", STRUCT,
                        List.of(
                                new Field("a", new FieldType(true, Schemas.INT64, null), null),
                                new Field("b", new FieldType(true, Schemas.UTF8, null), null)),
                        Map.of("a", 1L, "b", "x")));
    }
}
