// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * {@code dynamic_filter_echo(count BIGINT [const], batch_size := 100)} —
 * emits descending integers and echoes the *current* filter received via
 * per-tick {@code custom_metadata}. Top-N {@code ORDER BY n LIMIT K} on the
 * descending stream causes DuckDB's heap to tighten the filter every batch,
 * exercising the dynamic-filter wire path
 * ({@code vgi_pushdown_filters}, base64 IPC bytes).
 */
public final class DynamicFilterEchoFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("pushed_filters", Schemas.UTF8));

    @Override public String name() { return "dynamic_filter_echo"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Generates descending integers, echoes dynamic tick filter per batch")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true);
    }
    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override protected long defaultBatchSize() { return 100L; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        long batchSize = params.arguments().namedLong("batch_size", 100L);
        // Init-time filter — overridden each tick by the dynamic filter
        // payload arriving in custom_metadata. Use the Python-repr-style
        // representation so the dynamic_filter.test LIKE assertions match
        // (`pushed_filters LIKE '%ConstantFilter(n <%'`).
        byte[] pfBytes = params.pushdownFilters();
        String initFilter = pfBytes == null
                ? PushdownFilters.empty().formatRepr()
                : PushdownFiltersDecoder.decode(pfBytes, params.joinKeys()).formatRepr();
        return new State((int) count, (int) batchSize, initFilter, params.joinKeys());
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int total;
        public int batchSize;
        public int produced;
        public String currentFilter;
        public List<byte[]> joinKeysIpc;

        public State() {}
        State(int total, int batchSize, String currentFilter, List<byte[]> joinKeysIpc) {
            this.total = total;
            this.batchSize = batchSize;
            this.produced = 0;
            this.currentFilter = currentFilter;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            // Should never be reached — we always go through the metadata
            // overload below — but keep a sane fallback for safety.
            emitNextBatch(out);
        }

        @Override
        public void produceTick(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            Map<String, String> meta = input == null ? Map.of() : input.customMetadata();
            String encoded = meta == null ? null : meta.get("vgi_pushdown_filters");
            if (encoded != null && !encoded.isEmpty()) {
                try {
                    byte[] bytes = Base64.getDecoder().decode(encoded);
                    PushdownFilters pf = PushdownFiltersDecoder.decode(
                            bytes, joinKeysIpc == null ? List.of() : joinKeysIpc);
                    currentFilter = pf.formatRepr();
                } catch (Exception ignore) {
                    // Best-effort — fall back to the prior filter on decode error.
                }
            }
            emitNextBatch(out);
        }

        private void emitNextBatch(OutputCollector out) {
            if (produced >= total) { out.finish(); return; }
            int n = Math.min(batchSize, total - produced);
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector nv = (BigIntVector) root.getVector("n");
            VarCharVector pv = (VarCharVector) root.getVector("pushed_filters");
            Text filterText = new Text(currentFilter);
            for (int i = 0; i < n; i++) {
                long row = total - 1 - (produced + i);
                nv.setSafe(i, row);
                pv.setSafe(i, filterText);
            }
            root.setRowCount(n);
            out.emit(root);
            produced += n;
        }
    }
}
