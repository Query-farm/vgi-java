// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example;

import farm.query.vgi.SettingSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.Worker;
import farm.query.vgi.example.scalar.AddValuesFunction;
import farm.query.vgi.example.scalar.BinaryPacketFunction;
import farm.query.vgi.example.scalar.AnyMixedFunctions;
import farm.query.vgi.example.scalar.BernoulliFunction;
import farm.query.vgi.example.scalar.ConcatValuesFunctions;
import farm.query.vgi.example.scalar.ConditionalMessageFunction;
import farm.query.vgi.example.scalar.DoubleFunction;
import farm.query.vgi.example.scalar.FormatNumberFunctions;
import farm.query.vgi.example.scalar.GeoCentroidFixedFunction;
import farm.query.vgi.example.scalar.GeoCentroidListFunction;
import farm.query.vgi.example.scalar.GeoCentroidStructFunction;
import farm.query.vgi.example.scalar.GeoDistanceFixedFunction;
import farm.query.vgi.example.scalar.GeoDistanceListFunction;
import farm.query.vgi.example.scalar.GeoDistanceStructFunction;
import farm.query.vgi.example.scalar.HashSeedFunction;
import farm.query.vgi.example.scalar.MultiplyBySettingFunction;
import farm.query.vgi.example.scalar.MultiplyFunction;
import farm.query.vgi.example.scalar.NullHandlingFunction;
import farm.query.vgi.example.scalar.PairTypeFunctions;
import farm.query.vgi.example.scalar.RandomBytesFunction;
import farm.query.vgi.example.scalar.ReturnSecretValueFunction;
import farm.query.vgi.example.scalar.RandomIntFunction;
import farm.query.vgi.example.scalar.SumValuesFunction;
import farm.query.vgi.example.scalar.TypeInfoFunctions;
import farm.query.vgi.example.scalar.UnnestTensorFunction;
import farm.query.vgi.example.scalar.UpperCaseFunction;
import farm.query.vgi.example.scalar.WhoAmIFunction;
import farm.query.vgi.example.table.DoubleSequenceFunction;
import farm.query.vgi.example.table.DynamicFilterEchoFunction;
import farm.query.vgi.example.table.FilterEchoFunction;
import farm.query.vgi.example.table.FilterEchoPartitionedFunction;
import farm.query.vgi.example.table.GeneratorExceptionFunction;
import farm.query.vgi.example.table.LoggingGeneratorFunction;
import farm.query.vgi.example.table.MakePairsFunctions;
import farm.query.vgi.example.table.MakeSeriesFunctions;
import farm.query.vgi.example.table.RepeatValueFunctions;
import farm.query.vgi.example.table.NestedSequenceFunction;
import farm.query.vgi.example.table.ProfilingDemoFunction;
import farm.query.vgi.example.table.OrderEchoFunction;
import farm.query.vgi.example.table.PartitionedOrderModeFunctions;
import farm.query.vgi.example.table.PartitionedSequenceFunction;
import farm.query.vgi.example.table.ProjectedDataFunction;
import farm.query.vgi.example.table.SampleEchoFunction;
import farm.query.vgi.example.table.SlowCancellableFunction;
import farm.query.vgi.example.table.StructSettingsFunction;
import farm.query.vgi.example.table.NamedParamsEchoFunction;
import farm.query.vgi.example.table.SequenceFunction;
import farm.query.vgi.example.table.SettingsAwareFunction;
import farm.query.vgi.example.table.TenThousandFunction;
import farm.query.vgi.example.aggregate.AvgFunction;
import farm.query.vgi.example.aggregate.CountFunction;
import farm.query.vgi.example.aggregate.GenericSumFunction;
import farm.query.vgi.example.aggregate.ListAggFunction;
import farm.query.vgi.example.aggregate.PercentileFunction;
import farm.query.vgi.example.aggregate.StubAggregates;
import farm.query.vgi.example.aggregate.SumAllFunction;
import farm.query.vgi.example.aggregate.SumFunction;
import farm.query.vgi.example.aggregate.WeightedSumFunction;
import farm.query.vgi.example.tableinout.EchoFunction;
import farm.query.vgi.example.tableinout.BufferInputFunction;
import farm.query.vgi.example.tableinout.FilterBySettingFunction;
import farm.query.vgi.example.tableinout.SlowCancellableInoutFunction;
import farm.query.vgi.example.tableinout.UnnestTensorRowsFunction;
import farm.query.vgi.example.tableinout.DistributedSumFunction;
import farm.query.vgi.example.tableinout.ExceptionFinalizeFunction;
import farm.query.vgi.example.tableinout.ExceptionProcessFunction;
import farm.query.vgi.example.tableinout.RepeatInputsFunction;
import farm.query.vgi.example.tableinout.SumAllColumnsFunction;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.View;

public final class Main {

    private Main() {}

    private static java.util.Map<String, String> clampDefaults() {
        java.util.LinkedHashMap<String, String> m = new java.util.LinkedHashMap<>();
        m.put("lo", "0");
        m.put("hi", "100");
        return m;
    }

    /** Build a Field with optional {@code comment} and {@code default} metadata
     *  (read by the C++ extension at vgi_catalog_api.cpp:2061-2078). */
    private static Field col(String name,
                ArrowType type,
                boolean nullable,
                String comment, String defaultExpr) {
        java.util.LinkedHashMap<String, String> md = new java.util.LinkedHashMap<>();
        if (comment != null) md.put("comment", comment);
        if (defaultExpr != null) md.put("default", defaultExpr);
        return new Field(name,
                new FieldType(nullable, type, null,
                        md.isEmpty() ? null : md),
                null);
    }

    /** {@link #col(String, ArrowType, boolean, String, String) col} with no metadata. */
    private static Field col(String name,
                ArrowType type,
                boolean nullable) {
        return col(name, type, nullable, null, null);
    }

    /** Composite-struct row_id field for rowid_struct table. */
    private static Field rowidStructField() {
        return new Field("row_id",
                new FieldType(false,
                        new ArrowType.Struct(),
                        null,
                        Map.of("is_row_id", "true")),
                List.of(
                        Schemas.nullable("a", Schemas.INT64),
                        Schemas.nullable("b", Schemas.UTF8)));
    }

    /** Build a Field with the {@code is_row_id} metadata key, marking it as
     *  DuckDB's virtual {@code rowid} for this table (see C++ catalog
     *  binding at vgi_catalog_api.cpp:1684). */
    private static Field rowIdCol(String name,
                ArrowType type) {
        return new Field(name,
                new FieldType(false, type, null,
                        Map.of("is_row_id", "true")),
                null);
    }

    /** Build a Field with a generated_expression metadata key. */
    private static Field genCol(String name,
                ArrowType type,
                boolean nullable,
                String expr) {
        return new Field(name,
                new FieldType(nullable, type, null,
                        Map.of("generated_expression", expr)),
                null);
    }

    /** Serialize a list of fields as a TableInfo.columns IPC blob. */
    private static byte[] cols(Field... fields) {
        return SchemaUtil.serializeSchema(
                new Schema(List.of(fields)));
    }

    /** Build a CatalogTable backed by the {@code _table_data} canned-data
     *  fixture if it knows about {@code name}; otherwise metadata-only (SELECT
     *  fails cleanly rather than crashing on a column-count mismatch).
     */
    private static CatalogTable stubTable(String schema, String name, String comment,
                Field... fields) {
        boolean hasData = farm.query.vgi.example.table.CannedDataFunction.has(name);
        if (hasData) {
            return new CatalogTable(
                    schema, name, cols(fields), comment, Map.of(),
                    "_table_data", List.of((Object) name), Map.of(),
                    null, null, false, /*inlineScanFunction=*/true);
        }
        return new CatalogTable(
                schema, name, cols(fields), comment, Map.of(),
                null, List.of(), Map.of(),
                null, null, false, false);
    }

    private static Schema vgiExampleSecretSchema() {
        var redact = Map.of("redact", "true");
        return new Schema(List.of(
                new Field("secret_string",
                        new FieldType(true, Schemas.UTF8, null, redact), null),
                new Field("api_key",
                        new FieldType(true, Schemas.UTF8, null, redact), null),
                Schemas.nullable("port", Schemas.INT64),
                Schemas.nullable("use_ssl", Schemas.BOOL),
                Schemas.nullable("timeout", Schemas.FLOAT64)));
    }

    public static void main(String[] args) {
        // Capture stderr early — the launcher dup2's /dev/null over fd 2,
        // but System.setErr swaps Java's PrintStream wrapper to point at our
        // own file. Set VGI_WORKER_STDERR to inspect launcher-mode crashes.
        String stderrPath = System.getenv("VGI_WORKER_STDERR");
        if (stderrPath != null && !stderrPath.isEmpty()) {
            try {
                java.io.PrintStream ps = new java.io.PrintStream(
                        new java.io.FileOutputStream(stderrPath, true), true);
                System.setErr(ps);
            } catch (Exception ignore) {}
        }
        // Allow the test runner to override the catalog name + advertised
        // versions via env vars. Different VGI_*_WORKER binaries are
        // expected to advertise different catalog names; in this single-
        // binary build we let an env var swap the metadata.
        String catalogNameOverride = System.getenv("VGI_WORKER_CATALOG_NAME");
        String catalogName = catalogNameOverride != null && !catalogNameOverride.isEmpty()
                ? catalogNameOverride : "example";
        String implVer = System.getenv("VGI_WORKER_IMPLEMENTATION_VERSION");
        String dataSpec = System.getenv("VGI_WORKER_DATA_VERSION_SPEC");
        if (farm.query.vgi.example.table.AttachOptionsFixture.CATALOG_NAME.equals(catalogName)) {
            // Dedicated attach_options worker mode: register only echo_attach_options
            // and the 20 declared option specs. ATTACH 'attach_options' AS … must
            // see a catalog whose only function is echo_attach_options.
            Worker ao = Worker.builder()
                    .catalogName(catalogName)
                    .attachOptions(farm.query.vgi.example.table.AttachOptionsFixture
                            .declaredSpecs().toArray(new farm.query.vgi.AttachOptionSpec[0]))
                    .registerTable(new farm.query.vgi.example.table.EchoAttachOptionsFunction());
            runWorker(ao, args);
            return;
        }
        Worker w = Worker.builder()
                .catalogName(catalogName)
                .implementationVersion(implVer)
                .dataVersionSpec(dataSpec)
                .catalogComment("Example VGI catalog for testing")
                .catalogTags(Map.of(
                        "source", "vgi-fixture-worker",
                        "version", "1"));
        registerSettings(w);
        registerSecretTypes(w);
        registerScalars(w);
        registerTables(w);
        registerAggregates(w);
        registerTableInOuts(w);
        registerViews(w);
        registerCatalogTables(w);
        registerMacros(w);

        runWorker(w, args);
    }

    private static void registerSettings(Worker w) {
        w.settings(
                new SettingSpec("vgi_verbose_mode", "Enable verbose output",
                        Schemas.BOOL, Boolean.FALSE),
                new SettingSpec("greeting", "Custom greeting message",
                        Schemas.UTF8, "Hello"),
                new SettingSpec("multiplier", "Value multiplier",
                        Schemas.INT64, 1L),
                new SettingSpec("threshold", "Filter threshold",
                        Schemas.INT64, 0L),
                new SettingSpec("config", "Sequence configuration struct",
                        new ArrowType.Struct(),
                        List.of(
                Schemas.nullable("start", Schemas.INT64),
                Schemas.nullable("step", Schemas.INT64),
                Schemas.nullable("label", Schemas.UTF8))));
    }

    private static void registerSecretTypes(Worker w) {
        w.secretTypes(new farm.query.vgi.SecretTypeSpec(
                "vgi_example",
                "Example secret for VGI integration tests",
                vgiExampleSecretSchema()));
    }

    private static void registerScalars(Worker w) {
        w.registerScalars(List.of(
                new AddValuesFunction(),
                new ConditionalMessageFunction(),
                new DoubleFunction(),
                new HashSeedFunction(),
                new UpperCaseFunction(),
                new NullHandlingFunction(),
                new MultiplyBySettingFunction(),
                new RandomIntFunction(),
                new SumValuesFunction(),
                new WhoAmIFunction(),
                new GeoDistanceStructFunction(),
                new GeoDistanceListFunction(),
                new GeoDistanceFixedFunction(),
                new GeoCentroidStructFunction(),
                new GeoCentroidListFunction(),
                new GeoCentroidFixedFunction(),
                new BinaryPacketFunction(),
                new MultiplyFunction(),
                new FormatNumberFunctions.Default(),
                new FormatNumberFunctions.WithPrecision(),
                new FormatNumberFunctions.Full(),
                new ConcatValuesFunctions.IntVariant(),
                new ConcatValuesFunctions.StrVariant(),
                new TypeInfoFunctions.Int32(),
                new TypeInfoFunctions.Int64(),
                new TypeInfoFunctions.UInt32(),
                new TypeInfoFunctions.UInt64(),
                new TypeInfoFunctions.Varchar(),
                new PairTypeFunctions.IntInt(),
                new PairTypeFunctions.StrStr(),
                new PairTypeFunctions.IntStr(),
                new AnyMixedFunctions.IntVariant(),
                new AnyMixedFunctions.StrVariant(),
                new AnyMixedFunctions.SmartFormatInt(),
                new AnyMixedFunctions.SmartFormatStr(),
                new BernoulliFunction(),
                new RandomBytesFunction(),
                new ReturnSecretValueFunction(),
                new UnnestTensorFunction()));
    }

    private static void registerTables(Worker w) {
        w.registerTables(List.of(
                new SequenceFunction(),
                new farm.query.vgi.example.table.SecretDemoFunction(),
                new farm.query.vgi.example.table.ScopedSecretDemoFunction(),
                new farm.query.vgi.example.table.CannedDataFunction(),
                new farm.query.vgi.example.table.ConstantColumnsFunction(),
                new farm.query.vgi.example.table.RowIdSequenceFunction(),
                new farm.query.vgi.example.table.ProjReproFullSchemaFunction(),
                new farm.query.vgi.example.table.ProjReproFullSchemaFunction.Chunked(),
                new farm.query.vgi.example.table.ProjReproFullSchemaFunction.MultiWorker(),
                new farm.query.vgi.example.table.ProjReproFullSchemaFunction.Strict(),
                new farm.query.vgi.example.table.StubFunctions.ExpressionFilterTest(),
                new farm.query.vgi.example.table.StubFunctions.SpatialFilterExample(),
                new farm.query.vgi.example.table.StubFunctions.VersionedDataScan(),
                new farm.query.vgi.example.table.StubFunctions.ColorsScan(),
                new farm.query.vgi.example.table.StubFunctions.DepartmentsScan(),
                new farm.query.vgi.example.table.StubFunctions.EmployeesScan(),
                new farm.query.vgi.example.table.StubFunctions.ProductsScan(),
                new farm.query.vgi.example.table.StubFunctions.ProjectsScan(),
                new DoubleSequenceFunction(),
                new DynamicFilterEchoFunction(),
                new NamedParamsEchoFunction(),
                new GeneratorExceptionFunction(),
                new TenThousandFunction(),
                new FilterEchoFunction(),
                new FilterEchoPartitionedFunction(),
                new ProjectedDataFunction(),
                new NestedSequenceFunction(),
                new ProfilingDemoFunction(),
                new PartitionedSequenceFunction(),
                new PartitionedOrderModeFunctions.FixedOrder(),
                new PartitionedOrderModeFunctions.PreservesOrder(),
                new PartitionedOrderModeFunctions.NoOrderGuarantee(),
                new LoggingGeneratorFunction(),
                new OrderEchoFunction(),
                new SlowCancellableFunction(),
                new SampleEchoFunction(),
                new MakeSeriesFunctions.Count(),
                new MakeSeriesFunctions.Range(),
                new MakeSeriesFunctions.Step(),
                new MakeSeriesFunctions.Csv(),
                new MakeSeriesFunctions.FloatStep(),
                new RepeatValueFunctions.IntVariant(),
                new RepeatValueFunctions.StrVariant(),
                new MakePairsFunctions.IntVariant(),
                new MakePairsFunctions.StrVariant(),
                new MakePairsFunctions.MixedVariant(),
                new StructSettingsFunction(),
                new SettingsAwareFunction()));
    }

    private static void registerAggregates(Worker w) {
        w.registerAggregates(List.of(
                new SumFunction(),
                new CountFunction(),
                new AvgFunction(),
                new ListAggFunction(),
                new WeightedSumFunction(),
                new SumAllFunction(),
                new GenericSumFunction(),
                new PercentileFunction(),
                new StubAggregates.StreamingSum(),
                new StubAggregates.NestTensor(),
                new StubAggregates.WindowSum(),
                new StubAggregates.WindowSumBatch(),
                new StubAggregates.WindowMedian(),
                new StubAggregates.WindowListagg()));
    }

    private static void registerTableInOuts(Worker w) {
        w.registerTableInOuts(List.of(
                new EchoFunction(),
                new RepeatInputsFunction(),
                new ExceptionFinalizeFunction(),
                new ExceptionProcessFunction(),
                new SumAllColumnsFunction(),
                new DistributedSumFunction(),
                new BufferInputFunction(),
                new FilterBySettingFunction(),
                new SlowCancellableInoutFunction(),
                new UnnestTensorRowsFunction()));
    }

    private static void registerViews(Worker w) {
        w.registerView(new View(
                        "main", "first_ten",
                        "SELECT * FROM sequence(10)", "First 10 integers"))
                .registerView(new View(
                        "main", "even_numbers",
                        "SELECT * FROM sequence(100) WHERE n % 2 = 0",
                        "Even numbers from 0 to 98"))
                .registerView(new View(
                        "data", "small_numbers",
                        "SELECT * FROM example.main.make_series(10)",
                        "Numbers less than 10"));
    }

    /** Mirror the stats list to {@link farm.query.vgi.example.table.CannedDataFunction#putStats}
     *  so {@code _table_data}'s {@code statistics()} can serve them too. */
    private static List<farm.query.vgi.catalog.ColumnStatistics> tableStats(
            String tableName, farm.query.vgi.catalog.ColumnStatistics... stats) {
        List<farm.query.vgi.catalog.ColumnStatistics> list = List.of(stats);
        farm.query.vgi.example.table.CannedDataFunction.putStats(tableName, list);
        return list;
    }

    private static void registerCatalogTables(Worker w) {
        w.registerCatalogTable(CatalogTable.functionBacked(
                        "data", "ten_thousand_table",
                        SchemaUtil.serializeSchema(
                new Schema(List.of(
                Schemas.nullable("n", Schemas.INT64)))),
                        "Function-backed table over the no-arg ten_thousand function",
                        "ten_thousand"))
                .registerCatalogTable(new CatalogTable(
                        "data", "numbers",
                        SchemaUtil.serializeSchema(
                new Schema(List.of(
                Schemas.nullable("value", Schemas.INT64)))),
                        "First 100 integers (demonstrates explicit columns)",
                        Map.of(),
                        "make_series",
                        List.of((Object) 100L),
                        Map.of(),
                        100L, 100L, true, /*inlineScanFunction=*/false)
                        .withStatistics(List.of(
                farm.query.vgi.catalog.ColumnStatistics.ofInt64(
                "value", 0L, 99L, false, 100L))))
                .registerCatalogTable(new CatalogTable(
                        "data", "large_sequence",
                        SchemaUtil.serializeSchema(
                new Schema(List.of(
                Schemas.nullable("n", Schemas.INT64)))),
                        "A large sequence of integers from 0 to 1,000,000",
                        Map.of(),
                        "sequence",
                        List.of((Object) 1_000_001L),
                        Map.of(),
                        1_000_001L, 1_000_001L, true, /*inlineScanFunction=*/true))
                .registerCatalogTable(CatalogTable.functionBacked(
                "data", "cardinality_inlined_table",
                SchemaUtil.serializeSchema(
                new Schema(List.of(
                Schemas.nullable("n", Schemas.INT64)))),
                "Function-backed table with inlined cardinality (10000 rows)",
                "ten_thousand")
                        .withCardinality(10000L, 10000L))
                .registerCatalogTable(stubTable("data", "products",
                        "Product table with column defaults",
                        col("id", Schemas.INT64, false, "Unique product identifier", null),
                        col("name", Schemas.UTF8, true, "Product display name", "'unknown'"),
                        col("price", Schemas.FLOAT64, true, "Unit price in USD", "9.99"),
                        col("quantity", Schemas.INT64, true, null, "0"))
                        .withConstraints(
                List.of(List.of(0)),               // PK: id
                List.of(),
                List.of(),
                List.of())
                        .withStatistics(tableStats("products",
                farm.query.vgi.catalog.ColumnStatistics.ofInt64(
                "id", 1L, 100L, false, 100L),
                farm.query.vgi.catalog.ColumnStatistics.ofUtf8(
                "name", "Anvil", "Zebra Tape", false, 100L, false, 30L),
                farm.query.vgi.catalog.ColumnStatistics.ofInt64(
                "quantity", 0L, 10000L, true, null),
                farm.query.vgi.catalog.ColumnStatistics.ofFloat64(
                "price", 0.99d, 999.99d, false, null))))
                .registerCatalogTable(stubTable("data", "departments",
                        "Department reference table",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, false),
                        col("budget", Schemas.FLOAT64, true, null, "0"))
                        .withConstraints(
                List.of(List.of(0)),               // PK: id
                List.of(List.of(1)),               // UNIQUE: name
                List.of("budget >= 0"),                      // CHECK
                List.of())
                        .withStatistics(tableStats("departments",
                farm.query.vgi.catalog.ColumnStatistics.ofInt64(
                "id", 1L, 10L, false, 10L),
                farm.query.vgi.catalog.ColumnStatistics.ofUtf8(
                "name", "Accounting", "Sales", false, 10L, false, 20L),
                farm.query.vgi.catalog.ColumnStatistics.ofFloat64(
                "budget", 50000.0d, 500000.0d, false, 10L))))
                .registerCatalogTable(stubTable("data", "employees",
                        "Employee table with FK to departments",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, false),
                        col("email", Schemas.UTF8, false),
                        col("department_id", Schemas.INT64, true))
                        .withConstraints(
                List.of(List.of(0)),               // PK: id
                List.of(List.of(2)),               // UNIQUE: email
                List.of(),
                List.of(new CatalogTable.ForeignKey(
                List.of("department_id"),
                List.of("id"),
                "data", "departments"))))
                .registerCatalogTable(stubTable("data", "colors",
                        "Colors table with ENUM-derived statistics",
                        col("id", Schemas.INT64, false),
                        col("color", Schemas.UTF8, false),
                        col("hex_code", Schemas.UTF8, false))
                        .withStatistics(tableStats("colors",
                farm.query.vgi.catalog.ColumnStatistics.ofInt64(
                "id", 1L, 3L, false, 3L),
                farm.query.vgi.catalog.ColumnStatistics.ofUtf8(
                "color", "blue", "red", false, 3L, false, 5L),
                farm.query.vgi.catalog.ColumnStatistics.ofUtf8(
                "hex_code", "#0000FF", "#FF0000", false, 3L, false, 7L))))
                .registerCatalogTable(new CatalogTable(
                        "data", "funny_numbers",
                        cols(col("n", Schemas.INT64, true)),
                        "123456 integers; stats served by the sequence function, not the table",
                        Map.of(),
                        "sequence",
                        List.of((Object) 123456L),
                        Map.of(),
                        null, null, false, /*inlineScanFunction=*/true))
                .registerCatalogTable(stubTable("data", "volatile_numbers",
                        "Numbers with volatile stats (TTL=0, always re-fetched)",
                        col("value", Schemas.INT64, true))
                        .withStatistics(tableStats("volatile_numbers",
                farm.query.vgi.catalog.ColumnStatistics.ofInt64(
                "value", 0L, 99L, false, 100L))))
                .registerCatalogTable(stubTable("data", "generated_sequence",
                        "Table with generated columns backed by sequence(10)",
                        col("n", Schemas.INT64, true),
                        genCol("doubled", Schemas.INT64, true, "n * 2"),
                        genCol("label", Schemas.UTF8, true, "'item_' || n::VARCHAR")))
                .registerCatalogTable(stubTable("data", "projects",
                        "Projects with composite PK and FK to departments",
                        col("department_id", Schemas.INT64, false),
                        col("project_code", Schemas.UTF8, false),
                        col("title", Schemas.UTF8, false))
                        .withConstraints(
                List.of(List.of(0, 1)),            // PK: (department_id, project_code)
                List.of(),
                List.of(),
                List.of(new CatalogTable.ForeignKey(
                List.of("department_id"),
                List.of("id"),
                "data", "departments"))))
                .registerCatalogTable(stubTable("data", "rowid_first",
                        "Table with row_id at column index 0",
                        rowIdCol("row_id", Schemas.INT64),
                        col("name", Schemas.UTF8, true),
                        col("value", Schemas.UTF8, true)))
                .registerCatalogTable(stubTable("data", "rowid_middle",
                        "Table with row_id at column index 1",
                        col("name", Schemas.UTF8, true),
                        rowIdCol("row_id", Schemas.INT64),
                        col("value", Schemas.UTF8, true)))
                .registerCatalogTable(stubTable("data", "rowid_last",
                        "Table with row_id at column index 2",
                        col("name", Schemas.UTF8, true),
                        col("value", Schemas.UTF8, true),
                        rowIdCol("row_id", Schemas.INT64)))
                .registerCatalogTable(stubTable("data", "rowid_string",
                        "Table with string row_id",
                        rowIdCol("row_id", Schemas.UTF8),
                        col("payload", Schemas.UTF8, true)))
                .registerCatalogTable(new CatalogTable(
                        "data", "rowid_struct",
                        cols(rowidStructField(),
                col("payload", Schemas.UTF8, true)),
                        "Table with struct row_id", Map.of(),
                        "_table_data", List.of((Object) "rowid_struct"),
                        Map.of(), null, null, false, true))
                .registerCatalogTable(stubTable("data", "versioned_data",
                        "Versioned data table demonstrating time travel with schema evolution",
                        col("id", Schemas.INT64, false),
                        col("score", Schemas.FLOAT64, true)))
                .registerCatalogTable(stubTable("data", "versioned_data_v1",
                        "Versioned data — version 1 (id only)",
                        col("id", Schemas.INT64, false)))
                .registerCatalogTable(stubTable("data", "versioned_data_v2",
                        "Versioned data — version 2 (id, name, score, active)",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, true),
                        col("score", Schemas.FLOAT64, true),
                        col("active", Schemas.BOOL, true)))
                .registerCatalogTable(stubTable("data", "versioned_data_v3",
                        "Versioned data — version 3 (id, score)",
                        col("id", Schemas.INT64, false),
                        col("score", Schemas.FLOAT64, true)))
                .registerCatalogTable(stubTable("data", "versioned_constraints",
                        "Table with constraints that evolve across versions",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, false),
                        col("email", Schemas.UTF8, true),
                        col("department_id", Schemas.INT64, true))
                        .withConstraints(
                List.of(List.of(0)),               // PK: id
                List.of(List.of(2)),               // UNIQUE: email
                List.of(),
                List.of(new CatalogTable.ForeignKey(
                List.of("department_id"),
                List.of("id"),
                "data", "departments"))))
                .registerCatalogTable(stubTable("data", "versioned_constraints_v1",
                        "v1 (id, name)",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, true)))
                .registerCatalogTable(stubTable("data", "versioned_constraints_v2",
                        "v2 (id, name, email)",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, false),
                        col("email", Schemas.UTF8, false)))
                .registerCatalogTable(stubTable("data", "versioned_constraints_v3",
                        "v3 (id, name, email, department_id)",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, false),
                        col("email", Schemas.UTF8, false),
                        col("department_id", Schemas.INT64, true)))
                // Animals table for the "versioned_tables" worker — only
                // visible when that catalog is loaded. Schema evolves
                // across data versions (color column appears at 1.1.0).
                .registerCatalogTable(stubTable("main", "animals",
                        "Animals table (versioned)",
                        col("name", Schemas.UTF8, true),
                        col("legs", Schemas.INT64, true),
                        col("sound", Schemas.UTF8, true)))
                .registerCatalogTable(stubTable("main", "animals_v_1_0_0",
                        "Animals v1.0.0",
                        col("name", Schemas.UTF8, true),
                        col("legs", Schemas.INT64, true),
                        col("sound", Schemas.UTF8, true)))
                .registerCatalogTable(stubTable("main", "animals_v_1_1_0",
                        "Animals v1.1.0 — adds color",
                        col("name", Schemas.UTF8, true),
                        col("legs", Schemas.INT64, true),
                        col("sound", Schemas.UTF8, true),
                        col("color", Schemas.UTF8, true)))
                .registerCatalogTable(stubTable("main", "animals_v_2_0_0",
                        "Animals v2.0.0",
                        col("name", Schemas.UTF8, true),
                        col("legs", Schemas.INT64, true),
                        col("sound", Schemas.UTF8, true),
                        col("color", Schemas.UTF8, true)))
                .registerCatalogTable(stubTable("main", "plants",
                        "Plants table (versioned, appears at 2.0.0)",
                        col("name", Schemas.UTF8, true),
                        col("kind", Schemas.UTF8, true),
                        col("height_m", Schemas.FLOAT64, true)))
                .registerCatalogTable(stubTable("main", "plants_v_2_0_0",
                        "Plants v2.0.0",
                        col("name", Schemas.UTF8, true),
                        col("kind", Schemas.UTF8, true),
                        col("height_m", Schemas.FLOAT64, true)))
                .registerCatalogTable(stubTable("main", "plants_v_3_0_0",
                        "Plants v3.0.0",
                        col("name", Schemas.UTF8, true),
                        col("kind", Schemas.UTF8, true),
                        col("height_m", Schemas.FLOAT64, true)));
    }

    private static void registerMacros(Worker w) {
        w.registerMacro(new Macro(
                        "main", "vgi_multiply", MacroType.SCALAR,
                        List.of("x", "y"), "x * y", "Multiply two values"))
                .registerMacro(new Macro(
                        "main", "vgi_clamp", MacroType.SCALAR,
                        List.of("val", "lo", "hi"),
                        clampDefaults(),
                        "GREATEST(lo, LEAST(hi, val))",
                        "Clamp a value between lo and hi (defaults: 0..100)"))
                .registerMacro(new Macro(
                        "main", "vgi_range_table", MacroType.TABLE,
                        List.of("n"),
                        "SELECT * FROM range(n)",
                        "Table macro returning range of values"));
    }

    private static void runWorker(Worker w, String[] args) {
        boolean http = false;
        String host = "127.0.0.1";
        int port = 0;
        String unixSocket = null;
        long idleTimeoutMs = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--http" -> http = true;
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--unix" -> unixSocket = args[++i];
                case "--idle-timeout" ->
                        idleTimeoutMs = (long) (Double.parseDouble(args[++i]) * 1000.0);
                default -> { System.err.println("unknown arg: " + args[i]); System.exit(2); }
            }
        }
        if (unixSocket != null) {
            try { w.runUnixSocket(java.nio.file.Path.of(unixSocket), idleTimeoutMs); }
            catch (Exception e) { e.printStackTrace(); System.exit(1); }
        } else if (http) {
            try { w.runHttp(host, port); }
            catch (Exception e) { e.printStackTrace(); System.exit(1); }
        } else {
            w.runStdio();
        }
    }
}
