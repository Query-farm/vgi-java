// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.ParameterExtractor;
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
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().orElse(2048L);
        double increment = p.named("increment").asDouble().orElse(1.0);
        return new State(new BatchState(count, batchSize), increment);
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
