// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
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
public final class FilterEchoFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("s", Schemas.UTF8),
            Schemas.nullable("pushed_filters", Schemas.UTF8)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "filter_echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Echoes pushed-down filter predicates in output")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true);
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true),
                ArgSpec.named("batch_size", Schemas.INT64, "2048"));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        long batchSize = params.arguments().namedLong("batch_size", 2048L);
        byte[] pfBytes = params.pushdownFilters();
        PushdownFilters pf = pfBytes == null
                ? PushdownFilters.empty()
                : PushdownFiltersDecoder.decode(pfBytes, params.joinKeys());
        return new State(new BatchState(count, batchSize), pf.formatInline(), pfBytes,
                new CachedSchema(params.outputSchema()),
                params.joinKeys());
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String filterStr;
        public byte[] filterBytes;
        public CachedSchema outputSchema;
        public List<byte[]> joinKeysIpc;

        public State() {}

        State(BatchState batch, String filterStr, byte[] filterBytes, CachedSchema outputSchema,
                List<byte[]> joinKeysIpc) {
            this.batch = batch;
            this.filterStr = filterStr;
            this.filterBytes = filterBytes;
            this.outputSchema = outputSchema;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            emitOneBatch(out, filterStr, filterBytes);
        }

        @Override public void produceTick(farm.query.vgirpc.AnnotatedBatch input,
                                            OutputCollector out, CallContext ctx) {
            // DuckDB pushes dynamic filter updates (e.g. join-key IN filters
            // synthesized by the planner) as per-tick custom_metadata under
            // `vgi_pushdown_filters` (base64). Decode and let it shadow the
            // init-time filter for this batch.
            String fs = filterStr;
            byte[] fb = filterBytes;
            if (input != null) {
                java.util.Map<String, String> meta = input.customMetadata();
                String encoded = meta == null ? null : meta.get("vgi_pushdown_filters");
                if (encoded != null && !encoded.isEmpty()) {
                    try {
                        byte[] bytes = java.util.Base64.getDecoder().decode(encoded);
                        PushdownFilters pf = PushdownFiltersDecoder.decode(
                                bytes, joinKeysIpc == null ? List.of() : joinKeysIpc);
                        fs = pf.formatInline();
                        fb = bytes;
                    } catch (Exception ignore) { /* keep init-time filter */ }
                }
            }
            emitOneBatch(out, fs, fb);
        }

        private void emitOneBatch(OutputCollector out, String fs, byte[] fb) {
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
            if (fb != null) {
                work = FilterApplier.from(fb, joinKeysIpc).apply(work);
            }
            out.emit(VectorProjector.project(work, outputSchema.get()));
            batch.advance(n);
        }
    }
}
