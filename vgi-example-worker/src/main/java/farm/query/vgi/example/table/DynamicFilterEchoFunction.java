// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.Base64;
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
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().orElse(100L);
        return new State(params, (int) count, (int) batchSize);
    }

    public static final class State extends TableProducerState {
        public int total;
        public int batchSize;
        public int produced;
        public String currentFilter;

        public State() {}

        State(TableInitParams params, int total, int batchSize) {
            super(params);
            this.total = total;
            this.batchSize = batchSize;
            this.produced = 0;
            // Init-time filter, overridden each tick by the dynamic filter
            // payload arriving in custom_metadata. The Python-repr-style
            // rendering is what the dynamic_filter.test LIKE assertions match
            // (`pushed_filters LIKE '%ConstantFilter(n <%'`).
            this.currentFilter = filters.current().formatRepr();
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
                    filters.applyDelta(Base64.getDecoder().decode(encoded));
                    currentFilter = filters.current().formatRepr();
                } catch (Exception ignore) {
                    // Best-effort — fall back to the prior filter on decode error.
                }
            }
            emitNextBatch(out);
        }

        private void emitNextBatch(OutputCollector out) {
            if (produced >= total) { out.finish(); return; }
            int n = Math.min(batchSize, total - produced);
            int startProduced = produced;
            // Emit exactly the projected schema. The stream's schema is the
            // projection; a batch carrying the unprojected columns is not a
            // batch of that stream, and over HTTP it becomes the response's
            // schema, so the continuation-token batch that follows no longer
            // matches it and the client stops reading after the first batch.
            BatchUtil.emit(outputSchema, n, out, (root, rows, start) -> {
                BigIntVector nv = (BigIntVector) root.getVector("n");
                VarCharVector pv = (VarCharVector) root.getVector("pushed_filters");
                Text filterText = pv == null ? null : new Text(currentFilter);
                for (int i = 0; i < rows; i++) {
                    if (nv != null) nv.setSafe(i, total - 1L - (startProduced + i));
                    if (pv != null) pv.setSafe(i, filterText);
                }
            });
            produced += n;
        }
    }
}
