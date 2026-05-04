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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code sample_echo(count [, batch_size])} — emits per-row diagnostic
 * columns echoing whatever {@code TABLESAMPLE SYSTEM(pct) [REPEATABLE(seed)]}
 * DuckDB pushed down. Uses {@code -1.0 / -1} sentinel values when no
 * sampling hint reached the worker.
 */
public final class SampleEchoFunction implements TableFunction {

    private static final Schema FULL_SCHEMA = new Schema(List.of(
            new Field("n", new FieldType(true, Schemas.INT64, null), null),
            new Field("s", new FieldType(true, Schemas.UTF8, null), null),
            new Field("sample_percentage", new FieldType(true, Schemas.FLOAT64, null), null),
            new Field("sample_seed", new FieldType(true, Schemas.INT64, null), null)));
    private static final byte[] FULL_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(FULL_SCHEMA);

    @Override public String name() { return "sample_echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Echoes TABLESAMPLE pushdown hints in output columns")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withSamplingPushdown();
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
        double pct = params.tablesamplePercentage() == null ? -1.0 : params.tablesamplePercentage();
        long seed = params.tablesampleSeed() == null ? -1L : params.tablesampleSeed();
        return new State(new BatchState(count, batchSize), pct, seed,
                farm.query.vgi.internal.SchemaUtil.serializeSchema(params.outputSchema()),
                FilterApplier.from(params.pushdownFilters(), params.joinKeys()));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public double percentage;
        public long seed;
        public byte[] outputSchemaIpc;
        public FilterApplier filters;

        private transient Schema cachedSchema;

        public State() {}

        State(BatchState batch, double percentage, long seed, byte[] outputSchemaIpc,
                FilterApplier filters) {
            this.batch = batch;
            this.percentage = percentage;
            this.seed = seed;
            this.outputSchemaIpc = outputSchemaIpc;
            this.filters = filters;
        }

        private Schema schema() {
            if (cachedSchema == null) {
                cachedSchema = farm.query.vgi.internal.SchemaUtil.deserializeSchema(outputSchemaIpc);
            }
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
                    case "sample_percentage" -> {
                        Float8Vector f8 = (Float8Vector) v;
                        for (int i = 0; i < n; i++) f8.setSafe(i, percentage);
                    }
                    case "sample_seed" -> {
                        BigIntVector b = (BigIntVector) v;
                        for (int i = 0; i < n; i++) b.setSafe(i, seed);
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
