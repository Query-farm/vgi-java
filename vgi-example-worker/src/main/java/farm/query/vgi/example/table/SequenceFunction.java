// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
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

    private static void validate(java.util.Map<String, Object> named) {
        if (named.containsKey("positional_0") && named.get("positional_0") == null) {
            throw new IllegalArgumentException("count cannot be NULL");
        }
        if (named.get("count") == null && named.containsKey("count")) {
            throw new IllegalArgumentException("count cannot be NULL");
        }
        if (named.containsKey("batch_size") && named.get("batch_size") == null) {
            throw new IllegalArgumentException("batch_size cannot be NULL");
        }
        if (named.containsKey("increment") && named.get("increment") == null) {
            throw new IllegalArgumentException("increment cannot be NULL");
        }
        Object bs = named.get("batch_size");
        if (bs instanceof Number n && n.longValue() < 1L) {
            throw new IllegalArgumentException("batch_size must be >= 1, got " + n);
        }
        Object inc = named.get("increment");
        if (inc instanceof Number n && n.longValue() < 1L) {
            throw new IllegalArgumentException("increment must be >= 1, got " + n);
        }
    }

    @Override
    public farm.query.vgi.protocol.BindResponse onBind(farm.query.vgi.table.TableBindParams params) {
        validate(params.arguments().named());
        return super.onBind(params);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        validate(params.arguments().named());
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        long batchSize = params.arguments().namedLong("batch_size", 1000L);
        long increment = params.arguments().namedLong("increment", 1L);
        return new SequenceState(new BatchState(count, batchSize), increment,
                FilterApplier.from(params.pushdownFilters(), params.joinKeys()));
    }

    @Override
    public java.util.List<farm.query.vgi.catalog.ColumnStatistics> statistics(
            farm.query.vgi.table.TableBindParams params) {
        Object countObj = params.arguments().positional().isEmpty()
                ? null : params.arguments().positionalAt(0);
        if (!(countObj instanceof Number cn)) return null;
        long count = cn.longValue();
        long increment = params.arguments().namedLong("increment", 1L);
        if (count <= 0) return null;
        long max = (count - 1) * increment;
        return java.util.List.of(
                farm.query.vgi.catalog.ColumnStatistics.ofInt64("n", 0L, max, false, count));
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
