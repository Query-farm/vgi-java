// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end exercise of the VGI protocol from the <em>client</em> side.
 *
 * <p>Every piece needed to call a VGI worker from Java already shipped —
 * {@link VgiService} declares the wire surface, the {@code protocol} records
 * serialise both directions, and {@link RpcConnection#proxy} drives unary
 * <em>and</em> streaming methods. Nothing had ever assembled them: before this
 * test, {@code VgiService.class} appeared only in
 * {@code new RpcServer(VgiService.class, impl)}, so the client half of the
 * protocol was load-bearing-by-symmetry and unexercised.
 *
 * <p>This test walks the full scan path a query engine performs — attach,
 * discovery, bind, init, drain — against an in-process {@link Worker} over a
 * pipe transport, and asserts on the decoded rows. It is the reference for
 * anyone embedding VGI in a JVM consumer (Spark, Flink, Trino, a plain CLI).
 */
final class VgiClientRoundTripTest {

    // ------------------------------------------------------------------
    // Fixture: a table function emitting `count` rows of a single int64.
    // ------------------------------------------------------------------

    /** Emits {@code 0..count-1} as one int64 column named {@code i}. */
    public static final class SeqFunction extends CountdownTableFunction {

        static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("i", Schemas.INT64));

        @Override public String name() { return "seq"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Emits 0..count-1 as int64");
        }

        @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

        @Override public TableProducerState createProducer(TableInitParams params) {
            ParameterExtractor p = ParameterExtractor.of(params.arguments());
            long count = p.positional(0, "count").asLong().required();
            long batchSize = p.named("batch_size").asLong().orElse(2048L);
            return new State(new BatchState(count, batchSize));
        }

        /** Producer state; public no-arg ctor + public fields are the framework contract. */
        public static final class State extends TableProducerState {
            public BatchState batch;

            public State() {}

            State(BatchState batch) { this.batch = batch; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                BatchUtil.produceBatch(batch, OUTPUT_SCHEMA, null, out, (root, n, start) -> {
                    BigIntVector v = (BigIntVector) root.getVector("i");
                    for (int i = 0; i < n; i++) v.setSafe(i, start + i);
                });
            }
        }
    }

    // ------------------------------------------------------------------
    // The round trip
    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void scansATableFunctionEndToEnd() throws Exception {
        Worker worker = Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerTable(new SeqFunction());

        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker)) {
            VgiService vgi = h.client();

            // 1. ATTACH — the handle every later call echoes back.
            CatalogAttachResult attach = vgi.catalog_attach(
                    new CatalogAttachRequest("testcat", null, null, null), null);
            assertNotNull(attach.attach_opaque_data(), "attach must return a handle");
            assertEquals("main", attach.default_schema());
            byte[] handle = attach.attach_opaque_data();

            // 2. DISCOVERY — what a TableCatalog implementation would list.
            ItemsResponse schemas = vgi.catalog_schemas(handle, null);
            assertFalse(schemas.items().isEmpty(), "worker must advertise at least one schema");

            ItemsResponse functions =
                    vgi.catalog_schema_contents_functions(handle, "main", "TABLE_FUNCTION", null, null);
            assertEquals(1, functions.items().size(), "expected exactly the seq table function");

            // 3. BIND — resolve the output schema before any data moves.
            BindRequest bindRequest = new BindRequest(
                    "seq",                       // function_name
                    seqArgs(5L, null),           // arguments
                    "TABLE",                     // function_type
                    null,                        // input_schema (producer mode)
                    null,                        // settings
                    null,                        // secrets
                    handle,                      // attach_opaque_data
                    null,                        // transaction_opaque_data
                    false,                       // resolved_secrets_provided
                    null, null,                  // at_unit / at_value
                    null, null,                  // copy_from / copy_to
                    "main");                     // schema_name
            BindResponse bound = vgi.bind(bindRequest, null);

            Schema outputSchema = SchemaUtil.deserializeSchema(bound.output_schema());
            assertEquals(List.of("i"),
                    outputSchema.getFields().stream().map(Field::getName).toList());
            assertEquals(ArrowType.ArrowTypeID.Int,
                    outputSchema.getFields().get(0).getType().getTypeID());

            // 4. INIT — opens the producer stream; header carries execution_id.
            InitRequest initRequest = new InitRequest(
                    RecordCodec.serializeToBytes(bindRequest), // bind_call
                    bound.output_schema(),
                    bound.opaque_data(),
                    null,       // projection_ids — no projection pushdown here
                    null,       // pushdown_filters
                    null,       // join_keys
                    null,       // phase (producer mode)
                    null,       // execution_id — primary init, worker mints one
                    null,       // init_opaque_data
                    null, null, null, null,   // order-by hint
                    null, null,               // tablesample hint
                    null,       // finalize_state_id
                    null,
                    null,       // split_tokens — not a split init
                    null        // row_limit
            );      // substream_id

            RpcStream<? extends StreamState> stream = vgi.init(initRequest, null);
            GlobalInitResponse header = (GlobalInitResponse) stream.header();
            assertNotNull(header, "init must return a GlobalInitResponse header");
            assertNotNull(header.execution_id(), "worker must mint an execution_id");
            assertTrue(header.max_workers() >= 1, "max_workers must be positive");

            // 5. DRAIN — tick until the producer signals end of stream.
            List<Long> rows = drainInt64Column(stream, "i");
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L), rows);
        }
    }

    /**
     * A second scan on the same connection. Producer streams leave framing
     * state behind; if the drain does not consume the trailing EOS cleanly,
     * this call fails where the first succeeded.
     */
    @Test
    @Timeout(60)
    void reusesTheConnectionForASecondScan() throws Exception {
        Worker worker = Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerTable(new SeqFunction());

        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker)) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    new CatalogAttachRequest("testcat", null, null, null), null)
                    .attach_opaque_data();

            assertEquals(List.of(0L, 1L, 2L), scan(vgi, handle, 3L));
            assertEquals(List.of(0L, 1L, 2L, 3L), scan(vgi, handle, 4L));
            assertEquals(List.of(), scan(vgi, handle, 0L));
        }
    }

    /**
     * A scan whose row count spans several batches, so the drain loop is
     * exercised across more than one producer tick.
     */
    @Test
    @Timeout(60)
    void drainsAcrossMultipleBatches() throws Exception {
        Worker worker = Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerTable(new SeqFunction());

        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker)) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    new CatalogAttachRequest("testcat", null, null, null), null)
                    .attach_opaque_data();

            List<Long> rows = scan(vgi, handle, 5000L, 1000L);
            assertEquals(5000, rows.size());
            assertEquals(0L, rows.get(0));
            assertEquals(4999L, rows.get(rows.size() - 1));
        }
    }

    // ------------------------------------------------------------------
    // Client helpers — the shape a real JVM consumer would factor out.
    // ------------------------------------------------------------------

    private static List<Long> scan(VgiService vgi, byte[] handle, long count) throws IOException {
        return scan(vgi, handle, count, null);
    }

    private static List<Long> scan(VgiService vgi, byte[] handle, long count, Long batchSize)
            throws IOException {
        BindRequest bindRequest = new BindRequest(
                "seq", seqArgs(count, batchSize), "TABLE",
                null, null, null, handle, null, false,
                null, null, null, null, "main");
        BindResponse bound = vgi.bind(bindRequest, null);
        InitRequest initRequest = new InitRequest(
                RecordCodec.serializeToBytes(bindRequest), bound.output_schema(),
                bound.opaque_data(), null, null, null, null, null, null,
                null, null, null, null, null, null, null, null,
                    null,       // split_tokens — not a split init
                    null        // row_limit
            );
        return drainInt64Column(vgi.init(initRequest, null), "i");
    }

    /**
     * Bind arguments for {@code seq(count [, batch_size := n])}, encoded the
     * way DuckDB encodes them — {@link ArgumentsEncoder} is the client-side
     * mirror of the worker's {@code ArgumentsParser}.
     *
     * @param count     the positional {@code count} argument
     * @param batchSize optional named {@code batch_size}; omitted when null
     * @return IPC stream bytes for {@link BindRequest#arguments()}
     */
    private static byte[] seqArgs(long count, Long batchSize) {
        ArgumentsEncoder args = ArgumentsEncoder.builder().positional(count);
        if (batchSize != null) args.named("batch_size", batchSize);
        return args.encode();
    }

    /**
     * Drive a producer stream to exhaustion, collecting one int64 column.
     *
     * <p>{@link ClientStreamSession#tick()} raises {@link NoSuchElementException}
     * at end of stream rather than returning a sentinel, and a producer may
     * legitimately answer a tick with an empty batch before it finishes — so an
     * empty batch is "keep going", not "done".
     */
    private static List<Long> drainInt64Column(RpcStream<? extends StreamState> stream,
                                               String column) {
        List<Long> out = new ArrayList<>();
        ClientStreamSession<?> session = (ClientStreamSession<?>) stream;
        try {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = session.tick();
                } catch (NoSuchElementException endOfStream) {
                    break;
                }
                VectorSchemaRoot root = batch.root();
                BigIntVector v = (BigIntVector) root.getVector(column);
                for (int i = 0; i < root.getRowCount(); i++) {
                    out.add(v.get(i));
                }
            }
        } finally {
            session.close();
        }
        return out;
    }
}
