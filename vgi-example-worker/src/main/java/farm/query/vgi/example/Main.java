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
import farm.query.vgi.example.table.FilterEchoFunction;
import farm.query.vgi.example.table.GeneratorExceptionFunction;
import farm.query.vgi.example.table.LoggingGeneratorFunction;
import farm.query.vgi.example.table.MakePairsFunctions;
import farm.query.vgi.example.table.MakeSeriesFunctions;
import farm.query.vgi.example.table.RepeatValueFunctions;
import farm.query.vgi.example.table.NestedSequenceFunction;
import farm.query.vgi.example.table.OrderEchoFunction;
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

    public static void main(String[] args) {
        Worker w = Worker.builder()
                .catalogName("example")
                .catalogComment("Example VGI catalog for testing")
                .catalogTags(Map.of(
                        "source", "vgi-fixture-worker",
                        "version", "1"))
                .settings(
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
                                                new org.apache.arrow.vector.types.pojo.FieldType(true, Schemas.UTF8, null), null))))
                .registerScalar(new AddValuesFunction())
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
                .registerScalar(new UnnestTensorFunction())
                .registerTable(new SequenceFunction())
                .registerTable(new DoubleSequenceFunction())
                .registerTable(new NamedParamsEchoFunction())
                .registerTable(new GeneratorExceptionFunction())
                .registerTable(new TenThousandFunction())
                .registerTable(new FilterEchoFunction())
                .registerTable(new ProjectedDataFunction())
                .registerTable(new NestedSequenceFunction())
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
                .registerTable(new SettingsAwareFunction())
                .registerAggregate(new SumFunction())
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
                .registerAggregate(new StubAggregates.WindowListagg())
                .registerTableInOut(new EchoFunction())
                .registerTableInOut(new RepeatInputsFunction())
                .registerTableInOut(new ExceptionFinalizeFunction())
                .registerTableInOut(new ExceptionProcessFunction())
                .registerTableInOut(new SumAllColumnsFunction())
                .registerTableInOut(new DistributedSumFunction())
                .registerTableInOut(new BufferInputFunction())
                .registerTableInOut(new FilterBySettingFunction())
                .registerTableInOut(new SlowCancellableInoutFunction())
                .registerTableInOut(new UnnestTensorRowsFunction())
                .registerView(new Worker.View(
                        "main", "first_ten",
                        "SELECT * FROM sequence(10)", "First 10 integers"))
                .registerView(new Worker.View(
                        "main", "even_numbers",
                        "SELECT * FROM sequence(100) WHERE n % 2 = 0",
                        "Even numbers from 0 to 98"))
                .registerView(new Worker.View(
                        "data", "small_numbers",
                        "SELECT * FROM example.main.make_series(10)",
                        "Numbers less than 10"))
                .registerCatalogTable(Worker.CatalogTable.functionBacked(
                        "data", "ten_thousand_table",
                        farm.query.vgi.internal.SchemaUtil.serializeSchema(
                                new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(
                                        new org.apache.arrow.vector.types.pojo.Field("n",
                                                new org.apache.arrow.vector.types.pojo.FieldType(true,
                                                        Schemas.INT64, null), null)))),
                        "Function-backed table over the no-arg ten_thousand function",
                        "ten_thousand"))
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
                .registerMacro(new Worker.Macro(
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

        boolean http = false;
        String host = "127.0.0.1";
        int port = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--http" -> http = true;
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                default -> { System.err.println("unknown arg: " + args[i]); System.exit(2); }
            }
        }
        if (http) {
            try { w.runHttp(host, port); }
            catch (Exception e) { e.printStackTrace(); System.exit(1); }
        } else {
            w.runStdio();
        }
    }
}
