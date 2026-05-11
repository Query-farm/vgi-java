// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
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
            new Field("n", new FieldType(true, Schemas.INT64, null), null),
            new Field("s", new FieldType(true, Schemas.UTF8, null), null),
            new Field("pushed_filters", new FieldType(true, Schemas.UTF8, null), null)));
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
                new ArgSpec("batch_size", -1, Schemas.INT64, "", true, true, "2048", List.of(),
                        false, false));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        Object bsObj = params.arguments().named().get("batch_size");
        long batchSize = bsObj == null ? 2048L : ((Number) bsObj).longValue();
        byte[] pfBytes = params.pushdownFilters();
        PushdownFilters pf = pfBytes == null
                ? PushdownFilters.empty()
                : PushdownFiltersDecoder.decode(pfBytes, params.joinKeys());
        return new State(new BatchState(count, batchSize), pf.formatInline(), pfBytes,
                farm.query.vgi.internal.SchemaUtil.serializeSchema(params.outputSchema()),
                params.joinKeys());
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String filterStr;
        public byte[] filterBytes;
        public byte[] outputSchemaIpc;
        public List<byte[]> joinKeysIpc;

        private transient Schema cachedSchema;

        public State() {}

        State(BatchState batch, String filterStr, byte[] filterBytes, byte[] outputSchemaIpc,
                List<byte[]> joinKeysIpc) {
            this.batch = batch;
            this.filterStr = filterStr;
            this.filterBytes = filterBytes;
            this.outputSchemaIpc = outputSchemaIpc;
            this.joinKeysIpc = joinKeysIpc;
        }

        private Schema schema() {
            if (cachedSchema == null) {
                cachedSchema = farm.query.vgi.internal.SchemaUtil.deserializeSchema(outputSchemaIpc);
            }
            return cachedSchema;
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
                PushdownFilters pf = PushdownFiltersDecoder.decode(fb,
                        joinKeysIpc == null ? List.of() : joinKeysIpc);
                boolean[] mask = pf.evaluate(work);
                work = compact(work, mask);
            }
            VectorSchemaRoot emit = projectTo(work, schema());
            out.emit(emit);
            batch.advance(n);
        }

        private static VectorSchemaRoot projectTo(VectorSchemaRoot src, Schema target) {
            // If the requested schema matches the source, no-op.
            if (src.getSchema().equals(target)) return src;
            VectorSchemaRoot dst = VectorSchemaRoot.create(target, Allocators.root());
            dst.allocateNew();
            int rows = src.getRowCount();
            for (Field f : target.getFields()) {
                FieldVector dv = dst.getVector(f.getName());
                FieldVector svv = src.getVector(f.getName());
                if (svv == null) continue;
                for (int i = 0; i < rows; i++) dv.copyFromSafe(i, i, svv);
            }
            dst.setRowCount(rows);
            src.close();
            return dst;
        }

        private static VectorSchemaRoot compact(VectorSchemaRoot src, boolean[] mask) {
            int kept = 0;
            for (boolean b : mask) if (b) kept++;
            if (kept == src.getRowCount()) return src;
            VectorSchemaRoot dst = VectorSchemaRoot.create(src.getSchema(), Allocators.root());
            dst.allocateNew();
            int dstIdx = 0;
            for (int i = 0; i < mask.length; i++) {
                if (!mask[i]) continue;
                for (int c = 0; c < dst.getFieldVectors().size(); c++) {
                    FieldVector dv = dst.getVector(c);
                    FieldVector svv = src.getVector(c);
                    dv.copyFromSafe(i, dstIdx, svv);
                }
                dstIdx++;
            }
            dst.setRowCount(kept);
            src.close();
            return dst;
        }
    }
}
