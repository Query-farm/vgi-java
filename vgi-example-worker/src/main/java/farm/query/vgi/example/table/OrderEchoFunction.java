// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
            new Field("n", new FieldType(true, Schemas.INT64, null), null),
            new Field("s", new FieldType(true, Schemas.UTF8, null), null),
            new Field("order_column", new FieldType(true, Schemas.UTF8, null), null),
            new Field("order_direction", new FieldType(true, Schemas.UTF8, null), null),
            new Field("order_null_order", new FieldType(true, Schemas.UTF8, null), null),
            new Field("order_limit", new FieldType(true, Schemas.INT64, null), null)));
    private static final byte[] FULL_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(FULL_SCHEMA);

    @Override public String name() { return "order_echo"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Echoes ORDER BY + LIMIT pushdown hints in output columns")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true);
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, "", true, true, "10",
                        List.of(), false, false),
                new ArgSpec("batch_size", -1, Schemas.INT64, "", true, true, "2048",
                        List.of(), false, false));
    }
    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(FULL_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        Object bsObj = params.arguments().named().get("batch_size");
        long batchSize = bsObj == null ? 2048L : ((Number) bsObj).longValue();
        String col = params.orderByColumnName() == null || params.orderByColumnName().isEmpty()
                ? "(none)" : params.orderByColumnName();
        String dir = params.orderByDirection() == null || params.orderByDirection().isEmpty()
                ? "(none)" : params.orderByDirection();
        String nul = params.orderByNullOrder() == null || params.orderByNullOrder().isEmpty()
                ? "(none)" : params.orderByNullOrder();
        long limit = params.orderByLimit() == null ? -1L : params.orderByLimit();
        return new State(new BatchState(count, batchSize), col, dir, nul, limit,
                farm.query.vgi.internal.SchemaUtil.serializeSchema(params.outputSchema()),
                FilterApplier.from(params.pushdownFilters(), params.joinKeys()));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String orderCol;
        public String orderDir;
        public String orderNull;
        public long orderLimit;
        public byte[] outputSchemaIpc;
        public FilterApplier filters;

        private transient Schema cachedSchema;

        public State() {}

        State(BatchState batch, String orderCol, String orderDir, String orderNull, long orderLimit,
                byte[] outputSchemaIpc, FilterApplier filters) {
            this.batch = batch;
            this.orderCol = orderCol;
            this.orderDir = orderDir;
            this.orderNull = orderNull;
            this.orderLimit = orderLimit;
            this.outputSchemaIpc = outputSchemaIpc;
            this.filters = filters;
        }

        private Schema schema() {
            if (cachedSchema == null) cachedSchema =
                    farm.query.vgi.internal.SchemaUtil.deserializeSchema(outputSchemaIpc);
            return cachedSchema;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            Schema s = schema();
            VectorSchemaRoot root = VectorSchemaRoot.create(s, Allocators.root());
            root.allocateNew();
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
            root.setRowCount(n);
            if (filters != null) root = filters.apply(root);
            out.emit(root);
            batch.advance(n);
        }
    }
}
