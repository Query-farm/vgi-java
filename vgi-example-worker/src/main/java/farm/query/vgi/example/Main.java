// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example;

import farm.query.vgi.SettingSpec;
import farm.query.vgi.Worker;
import farm.query.vgi.example.scalar.AddValuesFunction;
import farm.query.vgi.example.scalar.BinaryPacketFunction;
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
import farm.query.vgi.example.scalar.RandomIntFunction;
import farm.query.vgi.example.scalar.SumValuesFunction;
import farm.query.vgi.example.scalar.UpperCaseFunction;
import farm.query.vgi.example.scalar.WhoAmIFunction;
import farm.query.vgi.example.table.DoubleSequenceFunction;
import farm.query.vgi.example.table.FilterEchoFunction;
import farm.query.vgi.example.table.GeneratorExceptionFunction;
import farm.query.vgi.example.table.LoggingGeneratorFunction;
import farm.query.vgi.example.table.NestedSequenceFunction;
import farm.query.vgi.example.table.ProjectedDataFunction;
import farm.query.vgi.example.table.SampleEchoFunction;
import farm.query.vgi.example.table.SlowCancellableFunction;
import farm.query.vgi.example.table.NamedParamsEchoFunction;
import farm.query.vgi.example.table.SequenceFunction;
import farm.query.vgi.example.table.TenThousandFunction;
import farm.query.vgi.example.aggregate.AvgFunction;
import farm.query.vgi.example.aggregate.CountFunction;
import farm.query.vgi.example.aggregate.GenericSumFunction;
import farm.query.vgi.example.aggregate.ListAggFunction;
import farm.query.vgi.example.aggregate.PercentileFunction;
import farm.query.vgi.example.aggregate.SumAllFunction;
import farm.query.vgi.example.aggregate.SumFunction;
import farm.query.vgi.example.aggregate.WeightedSumFunction;
import farm.query.vgi.example.tableinout.EchoFunction;
import farm.query.vgi.example.tableinout.DistributedSumFunction;
import farm.query.vgi.example.tableinout.ExceptionFinalizeFunction;
import farm.query.vgi.example.tableinout.ExceptionProcessFunction;
import farm.query.vgi.example.tableinout.RepeatInputsFunction;
import farm.query.vgi.example.tableinout.SumAllColumnsFunction;
import farm.query.vgi.types.Schemas;

import java.util.Map;

public final class Main {

    private Main() {}

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
                                Schemas.INT64, 0L))
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
                .registerTable(new SequenceFunction())
                .registerTable(new DoubleSequenceFunction())
                .registerTable(new NamedParamsEchoFunction())
                .registerTable(new GeneratorExceptionFunction())
                .registerTable(new TenThousandFunction())
                .registerTable(new FilterEchoFunction())
                .registerTable(new ProjectedDataFunction())
                .registerTable(new NestedSequenceFunction())
                .registerTable(new LoggingGeneratorFunction())
                .registerTable(new SlowCancellableFunction())
                .registerTable(new SampleEchoFunction())
                .registerAggregate(new SumFunction())
                .registerAggregate(new CountFunction())
                .registerAggregate(new AvgFunction())
                .registerAggregate(new ListAggFunction())
                .registerAggregate(new WeightedSumFunction())
                .registerAggregate(new SumAllFunction())
                .registerAggregate(new GenericSumFunction())
                .registerAggregate(new PercentileFunction())
                .registerTableInOut(new EchoFunction())
                .registerTableInOut(new RepeatInputsFunction())
                .registerTableInOut(new ExceptionFinalizeFunction())
                .registerTableInOut(new ExceptionProcessFunction())
                .registerTableInOut(new SumAllColumnsFunction())
                .registerTableInOut(new DistributedSumFunction());

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
