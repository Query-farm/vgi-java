// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code order_echo(count [, batch_size])} — diagnostic table function that
 * emits per-row columns echoing whatever
 * {@code ORDER BY ... [LIMIT k]} hints DuckDB pushed down via
 * {@code init.order_by_*}. Used by order_pushdown.test.
 */
public final class OrderEchoFunction implements TableFunction {

    private static final Schema FULL_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("s", Schemas.UTF8),
            Schemas.nullable("order_column", Schemas.UTF8),
            Schemas.nullable("order_direction", Schemas.UTF8),
            Schemas.nullable("order_null_order", Schemas.UTF8),
            Schemas.nullable("order_limit", Schemas.INT64)));
    private static final byte[] FULL_SCHEMA_IPC =
            SchemaUtil.serializeSchema(FULL_SCHEMA);

    private static final FunctionSpec SPEC = FunctionSpec.builder("order_echo")
            .metadata(FunctionMetadata.describe("Echoes ORDER BY + LIMIT pushdown hints in output")
                    .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true))
            .constArg("count", Schemas.INT64)
            .named("batch_size", Schemas.INT64, "2048")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(FULL_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        long batchSize = params.arguments().namedLong("batch_size", 2048L);
        String col = params.orderByColumnName() == null || params.orderByColumnName().isEmpty()
                ? "(none)" : params.orderByColumnName();
        String dir = params.orderByDirection() == null || params.orderByDirection().isEmpty()
                ? "(none)" : params.orderByDirection();
        String nul = params.orderByNullOrder() == null || params.orderByNullOrder().isEmpty()
                ? "(none)" : params.orderByNullOrder();
        long limit = params.orderByLimit() == null ? -1L : params.orderByLimit();
        return new State(new BatchState(count, batchSize), col, dir, nul, limit,
                new CachedSchema(params.outputSchema()),
                FilterApplier.from(params.pushdownFilters(), params.joinKeys()));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String orderCol;
        public String orderDir;
        public String orderNull;
        public long orderLimit;
        public CachedSchema outputSchema;
        public FilterApplier filters;

        public State() {}

        State(BatchState batch, String orderCol, String orderDir, String orderNull, long orderLimit,
                CachedSchema outputSchema, FilterApplier filters) {
            this.batch = batch;
            this.orderCol = orderCol;
            this.orderDir = orderDir;
            this.orderNull = orderNull;
            this.orderLimit = orderLimit;
            this.outputSchema = outputSchema;
            this.filters = filters;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            Schema s = outputSchema.get();
            BatchUtil.produceBatch(batch, s, filters, out, (root, n, start) -> {
                for (Field f : s.getFields()) {
                    FieldVector v = root.getVector(f.getName());
                    switch (f.getName()) {
                        case "n" -> {
                            BigIntVector b = (BigIntVector) v;
                            for (int i = 0; i < n; i++) b.setSafe(i, start + i);
                        }
                        case "s" -> {
                            VarCharVector vc = (VarCharVector) v;
                            for (int i = 0; i < n; i++) vc.setSafe(i, new Text("row_" + (start + i)));
                        }
                        case "order_column" -> {
                            VarCharVector vc = (VarCharVector) v;
                            Text t = new Text(orderCol);
                            for (int i = 0; i < n; i++) vc.setSafe(i, t);
                        }
                        case "order_direction" -> {
                            VarCharVector vc = (VarCharVector) v;
                            Text t = new Text(orderDir);
                            for (int i = 0; i < n; i++) vc.setSafe(i, t);
                        }
                        case "order_null_order" -> {
                            VarCharVector vc = (VarCharVector) v;
                            Text t = new Text(orderNull);
                            for (int i = 0; i < n; i++) vc.setSafe(i, t);
                        }
                        case "order_limit" -> {
                            BigIntVector b = (BigIntVector) v;
                            for (int i = 0; i < n; i++) b.setSafe(i, orderLimit);
                        }
                        default -> {}
                    }
                }
            });
        }
    }
}
