// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.List;

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
        byte[] pfBytes = params.pushdownFilters();
        PushdownFilters pf = pfBytes == null
                ? PushdownFilters.empty()
                : params.decodeFilters();
        return new State(new BatchState(count, batchSize), pf.formatInline(), pfBytes,
                params.bindOutputSchema(),
                new CachedSchema(params.outputSchema()),
                params.joinKeys());
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String filterStr;
        public byte[] filterBytes;
        public Schema bindOutputSchema;
        public List<byte[]> filterDeltas;
        public CachedSchema outputSchema;
        public List<byte[]> joinKeysIpc;
        public transient PushdownFilters currentFilters;

        public State() {}

        State(BatchState batch, String filterStr, byte[] filterBytes, Schema bindOutputSchema,
                CachedSchema outputSchema,
                List<byte[]> joinKeysIpc) {
            this.batch = batch;
            this.filterStr = filterStr;
            this.filterBytes = filterBytes;
            this.bindOutputSchema = bindOutputSchema;
            this.filterDeltas = new ArrayList<>();
            this.outputSchema = outputSchema;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            emitOneBatch(out, filterStr);
        }

        @Override public void produceTick(farm.query.vgirpc.AnnotatedBatch input,
                                            OutputCollector out, CallContext ctx) {
            // DuckDB pushes dynamic filter updates (e.g. join-key IN filters
            // synthesized by the planner) as per-tick custom_metadata under
            // `vgi_pushdown_filters` (base64). Decode and let it shadow the
            // init-time filter for this batch.
            String fs = filterStr;
            if (input != null) {
                java.util.Map<String, String> meta = input.customMetadata();
                String encoded = meta == null ? null : meta.get("vgi_pushdown_filters");
                if (encoded != null && !encoded.isEmpty()) {
                    try {
                        byte[] bytes = java.util.Base64.getDecoder().decode(encoded);
                        PushdownFilters pf = filters().applyDelta(bytes);
                        if (filterDeltas == null) filterDeltas = new ArrayList<>();
                        filterDeltas.add(bytes);
                        currentFilters = pf;
                        fs = currentFilters.formatInline();
                        filterStr = fs;
                    } catch (Exception ignore) { /* keep init-time filter */ }
                }
            }
            emitOneBatch(out, fs);
        }

        private PushdownFilters filters() {
            if (currentFilters == null) {
                currentFilters = filterBytes == null
                        ? PushdownFilters.empty()
                        : PushdownFiltersDecoder.decode(filterBytes, bindOutputSchema,
                                joinKeysIpc == null ? List.of() : joinKeysIpc,
                                PushdownFiltersDecoder.Capabilities.core());
                if (filterDeltas != null) {
                    for (byte[] delta : filterDeltas) currentFilters = currentFilters.applyDelta(delta);
                }
            }
            return currentFilters;
        }

        private void emitOneBatch(OutputCollector out, String fs) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector nv = (BigIntVector) work.getVector("n");
            VarCharVector sv = (VarCharVector) work.getVector("s");
            VarCharVector pv = (VarCharVector) work.getVector("pushed_filters");
            Text filterText = new Text(fs);
            for (int i = 0; i < n; i++) {
                long row = start + i;
                nv.setSafe(i, row);
                sv.setSafe(i, new Text("row_" + row));
                pv.setSafe(i, filterText);
            }
            work.setRowCount(n);
            PushdownFilters filters = filters();
            if (!filters.predicates().isEmpty()) {
                work = FilterApplier.compact(work, filters.evaluate(work));
            }
            out.emit(VectorProjector.project(work, outputSchema.get()));
            batch.advance(n);
        }
    }
}
