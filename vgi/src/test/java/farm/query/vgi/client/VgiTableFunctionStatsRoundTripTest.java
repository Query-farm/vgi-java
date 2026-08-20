// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.ColumnStatistics;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CardinalityResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The optimiser-facing half of a scan, driven from the client:
 * {@code table_function_cardinality} and {@code table_function_statistics}.
 *
 * <p>These two RPCs are what a query planner calls between bind and init to
 * decide how to schedule the scan, and they are the only ones that take a
 * <em>packed</em> request — a one-row IPC struct rather than a record — so a
 * client cannot call them at all without
 * {@link TableFunctionRequests}. Statistics also come back as raw bytes, which
 * only {@link ColumnStatisticsDecoder} can read.
 *
 * <p>The fixture reports mixed-type statistics on purpose: min/max ride a
 * sparse union whose member is chosen per column, so a single-type fixture
 * would never exercise the type-code mapping that makes the union work.
 */
final class VgiTableFunctionStatsRoundTripTest {

    /** Emits {@code (n, label)} rows and reports both cardinality and statistics. */
    public static final class LabelledSeqFunction extends CountdownTableFunction {

        static final Schema OUTPUT_SCHEMA = Schemas.of(
                Schemas.nullable("n", Schemas.INT64),
                Schemas.nullable("label", Schemas.UTF8));

        @Override public String name() { return "labelled_seq"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Emits 0..count-1 with a label column");
        }

        @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

        /**
         * Multi-column output, so the base class's single-column default does
         * not apply — these are hand-rolled, and mix an int64 column with a
         * utf8 one.
         */
        @Override public List<ColumnStatistics> statistics(TableBindParams params) {
            long count = ParameterExtractor.of(params.arguments())
                    .positional(0, "count").asLong().orElse(0L);
            if (count <= 0) return List.of();
            return List.of(
                    ColumnStatistics.ofInt64("n", 0L, count - 1, false, count),
                    ColumnStatistics.ofUtf8("label", "row-0", "row-" + (count - 1),
                            false, count, false, 32L));
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            long count = ParameterExtractor.of(params.arguments())
                    .positional(0, "count").asLong().required();
            return new State(new BatchState(count, 2048L));
        }

        /** Producer state; public no-arg ctor + public fields are the framework contract. */
        public static final class State extends TableProducerState {
            public BatchState batch;

            public State() {}

            State(BatchState batch) { this.batch = batch; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                BatchUtil.produceBatch(batch, OUTPUT_SCHEMA, null, out, (root, n, start) -> {
                    BigIntVector v = (BigIntVector) root.getVector("n");
                    VarCharVector label = (VarCharVector) root.getVector("label");
                    for (int i = 0; i < n; i++) {
                        v.setSafe(i, start + i);
                        label.setSafe(i, ("row-" + (start + i)).getBytes(StandardCharsets.UTF_8));
                    }
                });
            }
        }
    }

    @Test
    @Timeout(60)
    void reportsCardinalityAndStatisticsForABoundCall() throws Exception {
        Worker worker = Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerTable(new LabelledSeqFunction());

        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker)) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    CatalogAttachRequest.of("testcat", null, null, null), null)
                    .attach_opaque_data();

            BindRequest bindRequest = new BindRequest(
                    "labelled_seq", ArgumentsEncoder.positionalArgs(500L), "TABLE",
                    null, null, null, handle, null, false,
                    null, null, null, null, "main");
            BindResponse bound = vgi.bind(bindRequest, null);

            // One packed blob serves both calls — they read identical fields.
            byte[] request = TableFunctionRequests.forBind(bindRequest, bound.opaque_data());

            CardinalityResponse cardinality = vgi.table_function_cardinality(request);
            assertEquals(500L, cardinality.estimate());
            assertEquals(500L, cardinality.max());

            List<ColumnStatistics> stats =
                    ColumnStatisticsDecoder.decode(vgi.table_function_statistics(request));
            assertEquals(2, stats.size());

            ColumnStatistics n = stats.get(0);
            assertEquals("n", n.columnName());
            assertEquals(new ArrowType.Int(64, true), n.arrowType());
            assertEquals(0L, n.min());
            assertEquals(499L, n.max());
            assertEquals(500L, n.distinctCount());
            assertTrue(n.hasNotNull());
            assertEquals(false, n.hasNull());
            assertNull(n.containsUnicode());

            ColumnStatistics label = stats.get(1);
            assertEquals("label", label.columnName());
            assertEquals(new ArrowType.Utf8(), label.arrowType());
            assertEquals("row-0", label.min());
            assertEquals("row-499", label.max());
            assertEquals(false, label.containsUnicode());
            assertEquals(32L, label.maxStringLength());
        }
    }

    /**
     * The same two calls resolved from {@code bind_call} alone. A worker
     * prefers the opaque handle when it still holds the binding, but a pooled
     * or restarted one has to fall back to re-reading the bind call — so a
     * client that sends only the handle is one worker recycle away from losing
     * its statistics.
     */
    @Test
    @Timeout(60)
    void resolvesFromTheBindCallWhenNoOpaqueHandleIsSent() throws Exception {
        Worker worker = Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerTable(new LabelledSeqFunction());

        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker)) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    CatalogAttachRequest.of("testcat", null, null, null), null)
                    .attach_opaque_data();

            BindRequest bindRequest = new BindRequest(
                    "labelled_seq", ArgumentsEncoder.positionalArgs(4L), "TABLE",
                    null, null, null, handle, null, false,
                    null, null, null, null, "main");
            byte[] request = TableFunctionRequests.forBind(bindRequest, null);

            assertEquals(4L, vgi.table_function_cardinality(request).estimate());
            List<ColumnStatistics> stats =
                    ColumnStatisticsDecoder.decode(vgi.table_function_statistics(request));
            assertEquals(List.of("n", "label"), stats.stream().map(ColumnStatistics::columnName).toList());
            assertEquals(3L, stats.get(0).max());
        }
    }

    @Test
    void anEmptyStatisticsReplyDecodesToNoStatistics() {
        // "No statistics" is a normal answer (DuckDB falls back to its own
        // defaults), so the decoder must not treat empty bytes as corruption.
        assertTrue(ColumnStatisticsDecoder.decode(new byte[0]).isEmpty());
        assertTrue(ColumnStatisticsDecoder.decode(null).isEmpty());
    }
}
