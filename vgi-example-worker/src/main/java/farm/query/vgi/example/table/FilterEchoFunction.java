// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;


/**
 * {@code filter_echo(count [, batch_size])} — generates {@code count} rows
 * with a {@code pushed_filters} column echoing whatever filter predicates
 * DuckDB pushed down. Used to verify the filter-pushdown wire round-trip.
 *
 * <p>Filter pushdown is opt-in via {@link FunctionMetadata#filterPushdown}
 * + {@link FunctionMetadata#autoApplyFilters}. We don't apply filters in
 * Java — DuckDB applies them post-emit to the data we return.
 */
public final class FilterEchoFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("s", Schemas.UTF8),
            Schemas.nullable("pushed_filters", Schemas.UTF8));

    @Override public String name() { return "filter_echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Echoes pushed-down filter predicates in output")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true);
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected long defaultBatchSize() { return 2048L; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().orElse(2048L);
        return new State(params, new BatchState(count, batchSize));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String filterStr;

        public State() {}

        State(TableInitParams params, BatchState batch) {
            super(params);
            this.batch = batch;
            this.filterStr = filters.current().formatInline();
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            emitOneBatch(out);
        }

        @Override public void produceTick(farm.query.vgirpc.AnnotatedBatch input,
                                            OutputCollector out, CallContext ctx) {
            // DuckDB pushes dynamic filter updates (e.g. join-key IN filters
            // synthesized by the planner) as per-tick custom_metadata under
            // `vgi_pushdown_filters` (base64). Apply it on top of the init-time
            // filter; it shadows that filter from this batch on.
            if (input != null) {
                java.util.Map<String, String> meta = input.customMetadata();
                String encoded = meta == null ? null : meta.get("vgi_pushdown_filters");
                if (encoded != null && !encoded.isEmpty()) {
                    try {
                        filters.applyDelta(java.util.Base64.getDecoder().decode(encoded));
                        filterStr = filters.current().formatInline();
                    } catch (Exception ignore) { /* keep the prior filter */ }
                }
            }
            emitOneBatch(out);
        }

        private void emitOneBatch(OutputCollector out) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector nv = (BigIntVector) work.getVector("n");
            VarCharVector sv = (VarCharVector) work.getVector("s");
            VarCharVector pv = (VarCharVector) work.getVector("pushed_filters");
            Text filterText = new Text(filterStr);
            for (int i = 0; i < n; i++) {
                long row = start + i;
                nv.setSafe(i, row);
                sv.setSafe(i, new Text("row_" + row));
                pv.setSafe(i, filterText);
            }
            work.setRowCount(n);
            out.emit(VectorProjector.project(filters.apply(work), outputSchema));
            batch.advance(n);
        }
    }
}
