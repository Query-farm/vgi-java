// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public final class DoubleSequenceFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.FLOAT64));

    @Override public String name() { return "double_sequence"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates a sequence of floating-point numbers from 0 to n-1");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected List<ArgSpec> extraArgs() {
        return List.of(ArgSpec.named("increment", Schemas.FLOAT64, "1.0"));
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        long batchSize = params.arguments().namedLong("batch_size", 1000L);
        double increment = params.arguments().namedDouble("increment", 1.0);
        return new State(new BatchState(count, batchSize), increment);
    }

    @Override
    public java.util.List<farm.query.vgi.catalog.ColumnStatistics> statistics(
            farm.query.vgi.table.TableBindParams params) {
        Object countObj = params.arguments().positional().isEmpty()
                ? null : params.arguments().positionalAt(0);
        if (!(countObj instanceof Number cn)) return null;
        long count = cn.longValue();
        double increment = params.arguments().namedDouble("increment", 1.0);
        if (count <= 0) return null;
        double max = (count - 1) * increment;
        return java.util.List.of(
                farm.query.vgi.catalog.ColumnStatistics.ofFloat64("n", 0.0, max, false, count));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public double increment;

        public State() {}

        State(BatchState batch, double increment) {
            this.batch = batch;
            this.increment = increment;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            BatchUtil.produceBatch(batch, OUTPUT_SCHEMA, null, out, (root, n, start) -> {
                Float8Vector v = (Float8Vector) root.getVector("n");
                for (int i = 0; i < n; i++) v.setSafe(i, (start + i) * increment);
            });
        }
    }
}
