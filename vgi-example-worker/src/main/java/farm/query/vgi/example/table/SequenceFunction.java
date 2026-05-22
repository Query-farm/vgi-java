// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** {@code sequence(count BIGINT, batch_size := 1000, increment := 1)}. */
public final class SequenceFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

    @Override public String name() { return "sequence"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates a sequence of integers from 0 to n-1")
                .withPushdown(false, true, false).withCategories("generator", "utility");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected List<ArgSpec> extraArgs() {
        return List.of(ArgSpec.named("increment", Schemas.INT64, "1"));
    }

    private static void validate(ParameterExtractor p) {
        p.positional(0, "count").asLong().notNull();
        p.named("batch_size").asLong().ge(1).notNull();
        p.named("increment").asLong().ge(1).notNull();
    }

    @Override
    public farm.query.vgi.protocol.BindResponse onBind(farm.query.vgi.table.TableBindParams params) {
        validate(ParameterExtractor.of(params.arguments()));
        return super.onBind(params);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        validate(p);
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().ge(1).orElse(1000L);
        long increment = p.named("increment").asLong().ge(1).orElse(1L);
        return new SequenceState(new BatchState(count, batchSize), increment,
                FilterApplier.from(params.pushdownFilters(), params.joinKeys()));
    }

    public static final class SequenceState extends TableProducerState {
        public BatchState batch;
        public long increment;
        public FilterApplier filters;

        public SequenceState() {}

        SequenceState(BatchState batch, long increment, FilterApplier filters) {
            this.batch = batch;
            this.increment = increment;
            this.filters = filters;
        }

        @Override
        public void produceTick(OutputCollector out, CallContext ctx) {
            BatchUtil.produceBatch(batch, OUTPUT_SCHEMA, filters, out, (root, n, start) -> {
                BigIntVector v = (BigIntVector) root.getVector("n");
                for (int i = 0; i < n; i++) v.setSafe(i, (start + i) * increment);
            });
        }
    }
}
