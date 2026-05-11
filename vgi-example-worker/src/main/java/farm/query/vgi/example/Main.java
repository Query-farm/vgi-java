// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example;

import farm.query.vgi.SettingSpec;
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

import java.util.Map;

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
    private static org.apache.arrow.vector.types.pojo.Field col(String name,
                                                                  org.apache.arrow.vector.types.pojo.ArrowType type,
                                                                  boolean nullable,
                                                                  String comment, String defaultExpr) {
        java.util.LinkedHashMap<String, String> md = new java.util.LinkedHashMap<>();
        if (comment != null) md.put("comment", comment);
        if (defaultExpr != null) md.put("default", defaultExpr);
        return new org.apache.arrow.vector.types.pojo.Field(name,
                new org.apache.arrow.vector.types.pojo.FieldType(nullable, type, null,
                        md.isEmpty() ? null : md),
                null);
    }

    /** {@link #col(String, ArrowType, boolean, String, String) col} with no metadata. */
    private static org.apache.arrow.vector.types.pojo.Field col(String name,
                                                                  org.apache.arrow.vector.types.pojo.ArrowType type,
                                                                  boolean nullable) {
        return col(name, type, nullable, null, null);
    }

    /** Composite-struct row_id field for rowid_struct table. */
    private static org.apache.arrow.vector.types.pojo.Field rowidStructField() {
        return new org.apache.arrow.vector.types.pojo.Field("row_id",
                new org.apache.arrow.vector.types.pojo.FieldType(false,
                        new org.apache.arrow.vector.types.pojo.ArrowType.Struct(),
                        null,
                        java.util.Map.of("is_row_id", "true")),
                java.util.List.of(
                        new org.apache.arrow.vector.types.pojo.Field("a",
                                new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.INT64, null), null),
                        new org.apache.arrow.vector.types.pojo.Field("b",
                                new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.UTF8, null), null)));
    }

    /** Build a Field with the {@code is_row_id} metadata key, marking it as
     *  DuckDB's virtual {@code rowid} for this table (see C++ catalog
     *  binding at vgi_catalog_api.cpp:1684). */
    private static org.apache.arrow.vector.types.pojo.Field rowIdCol(String name,
                                                                       org.apache.arrow.vector.types.pojo.ArrowType type) {
        return new org.apache.arrow.vector.types.pojo.Field(name,
                new org.apache.arrow.vector.types.pojo.FieldType(false, type, null,
                        java.util.Map.of("is_row_id", "true")),
                null);
    }

    /** Build a Field with a generated_expression metadata key. */
    private static org.apache.arrow.vector.types.pojo.Field genCol(String name,
                                                                     org.apache.arrow.vector.types.pojo.ArrowType type,
                                                                     boolean nullable,
                                                                     String expr) {
        return new org.apache.arrow.vector.types.pojo.Field(name,
                new org.apache.arrow.vector.types.pojo.FieldType(nullable, type, null,
                        java.util.Map.of("generated_expression", expr)),
                null);
    }

    /** Serialize a list of fields as a TableInfo.columns IPC blob. */
    private static byte[] cols(org.apache.arrow.vector.types.pojo.Field... fields) {
        return farm.query.vgi.internal.SchemaUtil.serializeSchema(
                new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(fields)));
    }

    /** Build a CatalogTable backed by the {@code _table_data} canned-data
     *  fixture if it knows about {@code name}; otherwise metadata-only (SELECT
     *  fails cleanly rather than crashing on a column-count mismatch).
     */
    private static Worker.CatalogTable stubTable(String schema, String name, String comment,
                                                  org.apache.arrow.vector.types.pojo.Field... fields) {
        boolean hasData = farm.query.vgi.example.table.CannedDataFunction.has(name);
        if (hasData) {
            return new Worker.CatalogTable(
                    schema, name, cols(fields), comment, java.util.Map.of(),
                    "_table_data", java.util.List.of((Object) name), java.util.Map.of(),
                    null, null, false, /*inlineScanFunction=*/true);
        }
        return new Worker.CatalogTable(
                schema, name, cols(fields), comment, java.util.Map.of(),
                null, java.util.List.of(), java.util.Map.of(),
                null, null, false, false);
    }

    private static org.apache.arrow.vector.types.pojo.Schema vgiExampleSecretSchema() {
        var redact = java.util.Map.of("redact", "true");
        return new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(
                new org.apache.arrow.vector.types.pojo.Field("secret_string",
                        new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.UTF8, null, redact), null),
                new org.apache.arrow.vector.types.pojo.Field("api_key",
                        new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.UTF8, null, redact), null),
                new org.apache.arrow.vector.types.pojo.Field("port",
                        new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.INT64, null), null),
                new org.apache.arrow.vector.types.pojo.Field("use_ssl",
                        new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.BOOL, null), null),
                new org.apache.arrow.vector.types.pojo.Field("timeout",
                        new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.FLOAT64, null), null)));
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
                        new org.apache.arrow.vector.types.pojo.ArrowType.Struct(),
                        java.util.List.of(
                                new org.apache.arrow.vector.types.pojo.Field("start",
                                        new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.INT64, null), null),
                                new org.apache.arrow.vector.types.pojo.Field("step",
                                        new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.INT64, null), null),
                                new org.apache.arrow.vector.types.pojo.Field("label",
                                        new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.UTF8, null), null))));
    }

    private static void registerSecretTypes(Worker w) {
        w.secretTypes(new farm.query.vgi.SecretTypeSpec(
                "vgi_example",
                "Example secret for VGI integration tests",
                vgiExampleSecretSchema()));
    }

    private static void registerScalars(Worker w) {
        w.registerScalar(new AddValuesFunction())
                .registerScalar(new ConditionalMessageFunction())
                .registerScalar(new DoubleFunction())
                .registerScalar(new HashSeedFunction())
                .registerScalar(new UpperCaseFunction())
                .registerScalar(new NullHandlingFunction())
                .registerScalar(new MultiplyBySettingFunction())
                .registerScalar(new RandomIntFunction())
                .registerScalar(new SumValuesFunction())
                .registerScalar(new WhoAmIFunction())
                .registerScalar(new GeoDistanceStructFunction())
                .registerScalar(new GeoDistanceListFunction())
                .registerScalar(new GeoDistanceFixedFunction())
                .registerScalar(new GeoCentroidStructFunction())
                .registerScalar(new GeoCentroidListFunction())
                .registerScalar(new GeoCentroidFixedFunction())
                .registerScalar(new BinaryPacketFunction())
                .registerScalar(new MultiplyFunction())
                .registerScalar(new FormatNumberFunctions.Default())
                .registerScalar(new FormatNumberFunctions.WithPrecision())
                .registerScalar(new FormatNumberFunctions.Full())
                .registerScalar(new ConcatValuesFunctions.IntVariant())
                .registerScalar(new ConcatValuesFunctions.StrVariant())
                .registerScalar(new TypeInfoFunctions.Int32())
                .registerScalar(new TypeInfoFunctions.Int64())
                .registerScalar(new TypeInfoFunctions.UInt32())
                .registerScalar(new TypeInfoFunctions.UInt64())
                .registerScalar(new TypeInfoFunctions.Varchar())
                .registerScalar(new PairTypeFunctions.IntInt())
                .registerScalar(new PairTypeFunctions.StrStr())
                .registerScalar(new PairTypeFunctions.IntStr())
                .registerScalar(new AnyMixedFunctions.IntVariant())
                .registerScalar(new AnyMixedFunctions.StrVariant())
                .registerScalar(new AnyMixedFunctions.SmartFormatInt())
                .registerScalar(new AnyMixedFunctions.SmartFormatStr())
                .registerScalar(new BernoulliFunction())
                .registerScalar(new RandomBytesFunction())
                .registerScalar(new ReturnSecretValueFunction())
                .registerScalar(new UnnestTensorFunction());
    }

    private static void registerTables(Worker w) {
        w.registerTable(new SequenceFunction())
                .registerTable(new farm.query.vgi.example.table.SecretDemoFunction())
                .registerTable(new farm.query.vgi.example.table.ScopedSecretDemoFunction())
                .registerTable(new farm.query.vgi.example.table.CannedDataFunction())
                .registerTable(new farm.query.vgi.example.table.ConstantColumnsFunction())
                .registerTable(new farm.query.vgi.example.table.RowIdSequenceFunction())
                .registerTable(new farm.query.vgi.example.table.ProjReproFullSchemaFunction())
                .registerTable(new farm.query.vgi.example.table.ProjReproFullSchemaFunction.Chunked())
                .registerTable(new farm.query.vgi.example.table.ProjReproFullSchemaFunction.MultiWorker())
                .registerTable(new farm.query.vgi.example.table.ProjReproFullSchemaFunction.Strict())
                .registerTable(new farm.query.vgi.example.table.StubFunctions.ExpressionFilterTest())
                .registerTable(new farm.query.vgi.example.table.StubFunctions.SpatialFilterExample())
                .registerTable(new farm.query.vgi.example.table.StubFunctions.VersionedDataScan())
                .registerTable(new farm.query.vgi.example.table.StubFunctions.ColorsScan())
                .registerTable(new farm.query.vgi.example.table.StubFunctions.DepartmentsScan())
                .registerTable(new farm.query.vgi.example.table.StubFunctions.EmployeesScan())
                .registerTable(new farm.query.vgi.example.table.StubFunctions.ProductsScan())
                .registerTable(new farm.query.vgi.example.table.StubFunctions.ProjectsScan())
                .registerTable(new DoubleSequenceFunction())
                .registerTable(new DynamicFilterEchoFunction())
                .registerTable(new NamedParamsEchoFunction())
                .registerTable(new GeneratorExceptionFunction())
                .registerTable(new TenThousandFunction())
                .registerTable(new FilterEchoFunction())
                .registerTable(new FilterEchoPartitionedFunction())
                .registerTable(new ProjectedDataFunction())
                .registerTable(new NestedSequenceFunction())
                .registerTable(new ProfilingDemoFunction())
                .registerTable(new PartitionedSequenceFunction())
                .registerTable(new PartitionedOrderModeFunctions.FixedOrder())
                .registerTable(new PartitionedOrderModeFunctions.PreservesOrder())
                .registerTable(new PartitionedOrderModeFunctions.NoOrderGuarantee())
                .registerTable(new LoggingGeneratorFunction())
                .registerTable(new OrderEchoFunction())
                .registerTable(new SlowCancellableFunction())
                .registerTable(new SampleEchoFunction())
                .registerTable(new MakeSeriesFunctions.Count())
                .registerTable(new MakeSeriesFunctions.Range())
                .registerTable(new MakeSeriesFunctions.Step())
                .registerTable(new MakeSeriesFunctions.Csv())
                .registerTable(new MakeSeriesFunctions.FloatStep())
                .registerTable(new RepeatValueFunctions.IntVariant())
                .registerTable(new RepeatValueFunctions.StrVariant())
                .registerTable(new MakePairsFunctions.IntVariant())
                .registerTable(new MakePairsFunctions.StrVariant())
                .registerTable(new MakePairsFunctions.MixedVariant())
                .registerTable(new StructSettingsFunction())
                .registerTable(new SettingsAwareFunction());
    }

    private static void registerAggregates(Worker w) {
        w.registerAggregate(new SumFunction())
                .registerAggregate(new CountFunction())
                .registerAggregate(new AvgFunction())
                .registerAggregate(new ListAggFunction())
                .registerAggregate(new WeightedSumFunction())
                .registerAggregate(new SumAllFunction())
                .registerAggregate(new GenericSumFunction())
                .registerAggregate(new PercentileFunction())
                .registerAggregate(new StubAggregates.StreamingSum())
                .registerAggregate(new StubAggregates.NestTensor())
                .registerAggregate(new StubAggregates.WindowSum())
                .registerAggregate(new StubAggregates.WindowSumBatch())
                .registerAggregate(new StubAggregates.WindowMedian())
                .registerAggregate(new StubAggregates.WindowListagg());
    }

    private static void registerTableInOuts(Worker w) {
        w.registerTableInOut(new EchoFunction())
                .registerTableInOut(new RepeatInputsFunction())
                .registerTableInOut(new ExceptionFinalizeFunction())
                .registerTableInOut(new ExceptionProcessFunction())
                .registerTableInOut(new SumAllColumnsFunction())
                .registerTableInOut(new DistributedSumFunction())
                .registerTableInOut(new BufferInputFunction())
                .registerTableInOut(new FilterBySettingFunction())
                .registerTableInOut(new SlowCancellableInoutFunction())
                .registerTableInOut(new UnnestTensorRowsFunction());
    }

    private static void registerViews(Worker w) {
        w.registerView(new Worker.View(
                        "main", "first_ten",
                        "SELECT * FROM sequence(10)", "First 10 integers"))
                .registerView(new Worker.View(
                        "main", "even_numbers",
                        "SELECT * FROM sequence(100) WHERE n % 2 = 0",
                        "Even numbers from 0 to 98"))
                .registerView(new Worker.View(
                        "data", "small_numbers",
                        "SELECT * FROM example.main.make_series(10)",
                        "Numbers less than 10"));
    }

    private static void registerCatalogTables(Worker w) {
        w.registerCatalogTable(Worker.CatalogTable.functionBacked(
                        "data", "ten_thousand_table",
                        farm.query.vgi.internal.SchemaUtil.serializeSchema(
                                new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(
                                        new org.apache.arrow.vector.types.pojo.Field("n",
                                                new org.apache.arrow.vector.types.pojo.FieldType(true,
                                                        Schemas.INT64, null), null)))),
                        "Function-backed table over the no-arg ten_thousand function",
                        "ten_thousand"))
                .registerCatalogTable(new Worker.CatalogTable(
                        "data", "numbers",
                        farm.query.vgi.internal.SchemaUtil.serializeSchema(
                                new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(
                                        new org.apache.arrow.vector.types.pojo.Field("value",
                                                new org.apache.arrow.vector.types.pojo.FieldType(true,
                                                        Schemas.INT64, null), null)))),
                        "First 100 integers (demonstrates explicit columns)",
                        java.util.Map.of(),
                        "make_series",
                        java.util.List.of((Object) 100L),
                        java.util.Map.of(),
                        100L, 100L, true, /*inlineScanFunction=*/false))
                .registerCatalogTable(new Worker.CatalogTable(
                        "data", "large_sequence",
                        farm.query.vgi.internal.SchemaUtil.serializeSchema(
                                new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(
                                        new org.apache.arrow.vector.types.pojo.Field("n",
                                                new org.apache.arrow.vector.types.pojo.FieldType(true,
                                                        Schemas.INT64, null), null)))),
                        "A large sequence of integers from 0 to 1,000,000",
                        java.util.Map.of(),
                        "sequence",
                        java.util.List.of((Object) 1_000_001L),
                        java.util.Map.of(),
                        1_000_001L, 1_000_001L, true, /*inlineScanFunction=*/true))
                .registerCatalogTable(Worker.CatalogTable.functionBacked(
                                "data", "cardinality_inlined_table",
                                farm.query.vgi.internal.SchemaUtil.serializeSchema(
                                        new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(
                                                new org.apache.arrow.vector.types.pojo.Field("n",
                                                        new org.apache.arrow.vector.types.pojo.FieldType(true,
                                                                Schemas.INT64, null), null)))),
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
                                java.util.List.of(java.util.List.of(0)),               // PK: id
                                java.util.List.of(),
                                java.util.List.of(),
                                java.util.List.of()))
                .registerCatalogTable(stubTable("data", "departments",
                        "Department reference table",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, false),
                        col("budget", Schemas.FLOAT64, true, null, "0"))
                        .withConstraints(
                                java.util.List.of(java.util.List.of(0)),               // PK: id
                                java.util.List.of(java.util.List.of(1)),               // UNIQUE: name
                                java.util.List.of("budget >= 0"),                      // CHECK
                                java.util.List.of()))
                .registerCatalogTable(stubTable("data", "employees",
                        "Employee table with FK to departments",
                        col("id", Schemas.INT64, false),
                        col("name", Schemas.UTF8, false),
                        col("email", Schemas.UTF8, false),
                        col("department_id", Schemas.INT64, true))
                        .withConstraints(
                                java.util.List.of(java.util.List.of(0)),               // PK: id
                                java.util.List.of(java.util.List.of(2)),               // UNIQUE: email
                                java.util.List.of(),
                                java.util.List.of(new Worker.CatalogTable.ForeignKey(
                                        java.util.List.of("department_id"),
                                        java.util.List.of("id"),
                                        "data", "departments"))))
                .registerCatalogTable(stubTable("data", "colors",
                        "Colors table with ENUM-derived statistics",
                        col("id", Schemas.INT64, false),
                        col("color", Schemas.UTF8, false),
                        col("hex_code", Schemas.UTF8, false)))
                .registerCatalogTable(stubTable("data", "funny_numbers",
                        "123456 integers; stats served by the sequence function, not the table",
                        col("n", Schemas.INT64, true)))
                .registerCatalogTable(stubTable("data", "volatile_numbers",
                        "Numbers with volatile stats (TTL=0, always re-fetched)",
                        col("value", Schemas.INT64, true)))
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
                                java.util.List.of(java.util.List.of(0, 1)),            // PK: (department_id, project_code)
                                java.util.List.of(),
                                java.util.List.of(),
                                java.util.List.of(new Worker.CatalogTable.ForeignKey(
                                        java.util.List.of("department_id"),
                                        java.util.List.of("id"),
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
                .registerCatalogTable(new Worker.CatalogTable(
                        "data", "rowid_struct",
                        cols(rowidStructField(),
                                col("payload", Schemas.UTF8, true)),
                        "Table with struct row_id", java.util.Map.of(),
                        "_table_data", java.util.List.of((Object) "rowid_struct"),
                        java.util.Map.of(), null, null, false, true))
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
                                java.util.List.of(java.util.List.of(0)),               // PK: id
                                java.util.List.of(java.util.List.of(2)),               // UNIQUE: email
                                java.util.List.of(),
                                java.util.List.of(new Worker.CatalogTable.ForeignKey(
                                        java.util.List.of("department_id"),
                                        java.util.List.of("id"),
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
        w.registerMacro(new Worker.Macro(
                        "main", "vgi_multiply", Worker.MacroType.SCALAR,
                        java.util.List.of("x", "y"), "x * y", "Multiply two values"))
                .registerMacro(new Worker.Macro(
                        "main", "vgi_clamp", Worker.MacroType.SCALAR,
                        java.util.List.of("val", "lo", "hi"),
                        clampDefaults(),
                        "GREATEST(lo, LEAST(hi, val))",
                        "Clamp a value between lo and hi (defaults: 0..100)"))
                .registerMacro(new Worker.Macro(
                        "main", "vgi_range_table", Worker.MacroType.TABLE,
                        java.util.List.of("n"),
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
