// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Setting;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end exercise of VGI's <em>exchange</em> streaming shape from the
 * client side — the scalar-function path.
 *
 * <p>Its sibling {@link VgiClientRoundTripTest} covers <em>producer</em> mode
 * (client ticks, worker emits). Exchange is the other half: the client sends an
 * input batch and the worker answers with a 1:1 output batch, so the framing is
 * driven from both ends of the same transport at once.
 * {@link ClientStreamSession#exchange(AnnotatedBatch)} shipped alongside
 * {@code tick()} but had never been driven from a Java client — and driving
 * {@code tick()} for the first time turned up two defects
 * (see {@code ClientStreamHeaderTest} in vgi-rpc-java), so the untested half
 * deserved the same treatment.
 *
 * <p>Unlike a table function, a scalar bind <strong>must</strong> carry
 * {@code BindRequest.input_schema}: it is what
 * {@code VgiServiceImpl.bindScalar} resolves the overload against and what the
 * server casts each inbound batch to, and it is also how the framework tells an
 * exchange stream from a producer one ({@code RpcStream.isProducer()} is
 * "input schema has no fields").
 */
final class VgiScalarExchangeRoundTripTest {

    // ------------------------------------------------------------------
    // Fixtures: two scalars with different input/output shapes, so a second
    // bind on one connection is a genuinely different stream, not a replay.
    // ------------------------------------------------------------------

    /** {@code add_pair(a, b)} — sums two int64 columns, nulls propagate. */
    public static final class AddPairFunction extends ScalarFn {

        @Override public String name() { return "add_pair"; }
        @Override public String description() { return "Adds two int64 columns"; }

        public void compute(@Vector("a") BigIntVector a,
                            @Vector("b") BigIntVector b,
                            BigIntVector result) {
            for (int i = 0; i < a.getValueCount(); i++) {
                if (a.isNull(i) || b.isNull(i)) {
                    result.setNull(i);
                    continue;
                }
                result.setSafe(i, a.get(i) + b.get(i));
            }
        }
    }

    /** {@code shout(s)} — uppercases a utf8 column and appends {@code !}. */
    public static final class ShoutFunction extends ScalarFn {

        @Override public String name() { return "shout"; }
        @Override public String description() { return "Uppercases a string"; }

        public void compute(@Vector("s") VarCharVector s, VarCharVector result) {
            for (int i = 0; i < s.getValueCount(); i++) {
                if (s.isNull(i)) {
                    result.setNull(i);
                    continue;
                }
                String text = new String(s.get(i), StandardCharsets.UTF_8);
                result.setSafe(i, (text.toUpperCase() + "!").getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * {@code scale(x, factor)} — a const bind-time argument and a session
     * setting alongside the per-row column, so one call exercises all three
     * client-side encodings at once.
     */
    public static final class ScaleFunction extends ScalarFn {

        @Override public String name() { return "scale"; }
        @Override public String description() { return "Scales a column by a const factor plus a bias setting"; }

        public void compute(@Vector("x") BigIntVector x,
                            @Const("factor") long factor,
                            @Setting(value = "bias", default_ = "0") long bias,
                            BigIntVector result) {
            for (int i = 0; i < x.getValueCount(); i++) {
                if (x.isNull(i)) {
                    result.setNull(i);
                    continue;
                }
                result.setSafe(i, x.get(i) * factor + bias);
            }
        }
    }

    private static final Schema PAIR_INPUT = Schemas.of(
            Schemas.nullable("a", Schemas.INT64),
            Schemas.nullable("b", Schemas.INT64));

    private static final Schema TEXT_INPUT = Schemas.of(Schemas.nullable("s", Schemas.UTF8));

    private static final Schema SCALE_INPUT = Schemas.of(Schemas.nullable("x", Schemas.INT64));

    // ------------------------------------------------------------------
    // The round trip
    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void computesAScalarAcrossSeveralExchangeTurns() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            VgiService vgi = h.client();

            // 1. ATTACH — the handle every later call echoes back.
            CatalogAttachResult attach = vgi.catalog_attach(
                    new CatalogAttachRequest("testcat", null, null, null), null);
            assertNotNull(attach.attach_opaque_data(), "attach must return a handle");
            byte[] handle = attach.attach_opaque_data();

            // 2. DISCOVERY — a scalar is advertised under function kind "scalar".
            ItemsResponse functions =
                    vgi.catalog_schema_contents_functions(handle, "main", "scalar", null, null);
            assertFalse(functions.items().isEmpty(), "worker must advertise its scalars");

            // 3. BIND — the input schema is mandatory here (see the class doc).
            BindRequest bindRequest = scalarBind("add_pair", PAIR_INPUT, handle);
            BindResponse bound = vgi.bind(bindRequest, null);
            Schema outputSchema = SchemaUtil.deserializeSchema(bound.output_schema());
            assertEquals(List.of("result"),
                    outputSchema.getFields().stream().map(Field::getName).toList());

            // 4. INIT — opens the exchange stream; the header carries execution_id.
            RpcStream<? extends StreamState> stream = vgi.init(initFor(bindRequest, bound), null);
            GlobalInitResponse header = (GlobalInitResponse) stream.header();
            assertNotNull(header, "init must return a GlobalInitResponse header");
            assertNotNull(header.execution_id(), "worker must mint an execution_id");

            ClientStreamSession<?> session = (ClientStreamSession<?>) stream;
            try {
                // 5. EXCHANGE — several turns on one stream. Each turn is an
                // independent lockstep write/read, so a framing slip on turn N
                // shows up as a hang or a decode error on turn N+1.
                assertEquals(Arrays.asList(3L, 30L, 300L),
                        exchangeSums(session, new long[]{1, 10, 100}, new long[]{2, 20, 200}));
                assertEquals(Arrays.asList(0L),
                        exchangeSums(session, new long[]{-5}, new long[]{5}));
                assertEquals(Arrays.asList(11L, 22L),
                        exchangeSums(session, new long[]{1, 2}, new long[]{10, 20}));
            } finally {
                session.close();
            }
        }
    }

    /**
     * A zero-row input batch. The protocol admits one — an exchange batch is
     * classified by its metadata, not its row count (see {@code Wire.classify}),
     * so an empty batch is data with no rows rather than an end-of-stream
     * marker. DuckDB does not normally send one, but a JVM consumer draining an
     * empty partition easily can, and answering it must not desynchronise the
     * stream: the turns after it are the real assertion.
     */
    @Test
    @Timeout(60)
    void answersAZeroRowInputBatchAndKeepsGoing() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    new CatalogAttachRequest("testcat", null, null, null), null)
                    .attach_opaque_data();

            BindRequest bindRequest = scalarBind("add_pair", PAIR_INPUT, handle);
            BindResponse bound = vgi.bind(bindRequest, null);
            ClientStreamSession<?> session =
                    (ClientStreamSession<?>) vgi.init(initFor(bindRequest, bound), null);
            try {
                assertEquals(List.of(), exchangeSums(session, new long[]{}, new long[]{}));
                assertEquals(List.of(7L), exchangeSums(session, new long[]{3}, new long[]{4}));
                assertEquals(List.of(), exchangeSums(session, new long[]{}, new long[]{}));
                assertEquals(List.of(9L), exchangeSums(session, new long[]{4}, new long[]{5}));
            } finally {
                session.close();
            }
        }
    }

    /**
     * Two scalars, back to back, on one connection — plus a unary call after
     * each. Transport-reuse framing is where the producer path's bugs lived: a
     * stream that leaves a stale EOS (or an unread header) behind poisons the
     * <em>next</em> call, not its own, so the second bind/init/exchange and the
     * trailing unary call are what actually prove the first stream closed
     * cleanly.
     */
    @Test
    @Timeout(60)
    void reusesTheConnectionForASecondScalar() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    new CatalogAttachRequest("testcat", null, null, null), null)
                    .attach_opaque_data();

            BindRequest addBind = scalarBind("add_pair", PAIR_INPUT, handle);
            BindResponse addBound = vgi.bind(addBind, null);
            ClientStreamSession<?> adder =
                    (ClientStreamSession<?>) vgi.init(initFor(addBind, addBound), null);
            try {
                assertEquals(Arrays.asList(3L, 7L), exchangeSums(adder, new long[]{1, 3}, new long[]{2, 4}));
            } finally {
                adder.close();
            }

            // Unary call between the two streams: the classic canary for a
            // stream that did not consume its own trailing EOS.
            assertFalse(vgi.catalog_schemas(handle, null).items().isEmpty());

            BindRequest shoutBind = scalarBind("shout", TEXT_INPUT, handle);
            BindResponse shoutBound = vgi.bind(shoutBind, null);
            ClientStreamSession<?> shouter =
                    (ClientStreamSession<?>) vgi.init(initFor(shoutBind, shoutBound), null);
            try {
                assertEquals(Arrays.asList("HELLO!", "VGI!"), exchangeShouts(shouter, "hello", "vgi"));
                assertEquals(Arrays.asList("AGAIN!"), exchangeShouts(shouter, "again"));
            } finally {
                shouter.close();
            }

            assertFalse(vgi.catalog_schemas(handle, null).items().isEmpty());

            // And a third stream, to prove the second one closed as cleanly as
            // the first (two-in-a-row can pass on a one-shot leak).
            BindRequest thirdBind = scalarBind("add_pair", PAIR_INPUT, handle);
            BindResponse thirdBound = vgi.bind(thirdBind, null);
            ClientStreamSession<?> third =
                    (ClientStreamSession<?>) vgi.init(initFor(thirdBind, thirdBound), null);
            try {
                assertEquals(Arrays.asList(111L), exchangeSums(third, new long[]{100}, new long[]{11}));
            } finally {
                third.close();
            }
        }
    }

    /**
     * {@code close()} is idempotent and must not read a stream that has already
     * signalled EOS — the exchange-side counterpart of
     * {@code ClientStreamHeaderTest.closingAfterEndOfStreamDoesNotBlock}. An
     * exchange never sees EOS mid-conversation (the server answers every input
     * batch), so its {@code close()} is the <em>first</em> read past the last
     * answer; getting that wrong hangs rather than fails.
     */
    @Test
    @Timeout(60)
    void closingAnExchangeIsIdempotent() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    new CatalogAttachRequest("testcat", null, null, null), null)
                    .attach_opaque_data();

            BindRequest bindRequest = scalarBind("add_pair", PAIR_INPUT, handle);
            BindResponse bound = vgi.bind(bindRequest, null);
            ClientStreamSession<?> session =
                    (ClientStreamSession<?>) vgi.init(initFor(bindRequest, bound), null);
            assertEquals(Arrays.asList(5L), exchangeSums(session, new long[]{2}, new long[]{3}));
            session.close();
            session.close();

            // The connection is still usable afterwards.
            assertFalse(vgi.catalog_schemas(handle, null).items().isEmpty());
        }
    }

    /**
     * A bind that actually carries arguments and settings, encoded with
     * {@link ArgumentsEncoder} / {@link SettingsEncoder} — the client-side
     * mirrors of the worker's {@code ArgumentsParser} / {@code SettingsParser}.
     * The other cases here bind with null arguments, so this is what proves a
     * JVM client can drive a scalar that has a const parameter at all: the
     * const {@code factor} is read at bind time and the {@code bias} setting at
     * process time, and a wrong encoding of either shows up directly in the
     * arithmetic.
     */
    @Test
    @Timeout(60)
    void passesConstArgumentsAndSettingsThroughTheBind() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    new CatalogAttachRequest("testcat", null, null, null), null)
                    .attach_opaque_data();

            BindRequest bindRequest = new BindRequest(
                    "scale",
                    ArgumentsEncoder.positionalArgs(10L),          // factor := 10
                    "SCALAR",
                    SchemaUtil.serializeSchema(SCALE_INPUT),
                    SettingsEncoder.builder().setting("bias", 7L).encode(),
                    null, handle, null, false,
                    null, null, null, null, "main");
            BindResponse bound = vgi.bind(bindRequest, null);

            ClientStreamSession<?> session =
                    (ClientStreamSession<?>) vgi.init(initFor(bindRequest, bound), null);
            try {
                assertEquals(Arrays.asList(17L, 27L), exchangeScaled(session, 1L, 2L));
            } finally {
                session.close();
            }
        }
    }

    // ------------------------------------------------------------------
    // Client helpers — the shape a real JVM consumer would factor out.
    // ------------------------------------------------------------------

    private static Worker worker() {
        return Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerScalar(new AddPairFunction())
                .registerScalar(new ShoutFunction())
                .registerScalar(new ScaleFunction());
    }

    /**
     * A scalar {@link BindRequest}. Two fields differ from the table-function
     * bind in {@link VgiClientRoundTripTest}: {@code function_type} is
     * {@code SCALAR} (what the DuckDB extension sends), and {@code input_schema}
     * is populated — the columns the scalar receives per row.
     */
    private static BindRequest scalarBind(String function, Schema inputSchema, byte[] handle) {
        return new BindRequest(
                function,                                  // function_name
                null,                                      // arguments — no const args
                "SCALAR",                                  // function_type
                SchemaUtil.serializeSchema(inputSchema),   // input_schema (mandatory for scalars)
                null,                                      // settings
                null,                                      // secrets
                handle,                                    // attach_opaque_data
                null,                                      // transaction_opaque_data
                false,                                     // resolved_secrets_provided
                null, null,                                // at_unit / at_value
                null, null,                                // copy_from / copy_to
                "main");                                   // schema_name
    }

    private static InitRequest initFor(BindRequest bindRequest, BindResponse bound) {
        return new InitRequest(
                RecordCodec.serializeToBytes(bindRequest), // bind_call
                bound.output_schema(),
                bound.opaque_data(),
                null,       // projection_ids
                null,       // pushdown_filters
                null,       // join_keys
                null,       // phase — scalars have none
                null,       // execution_id — worker mints one
                null,       // init_opaque_data
                null, null, null, null,   // order-by hint
                null, null,               // tablesample hint
                null,       // finalize_state_id
                null);      // substream_id
    }

    /**
     * One exchange turn over {@code add_pair}: build the two-column input batch,
     * send it, and decode the {@code result} column of the answer.
     *
     * <p>The returned batch's root belongs to the session's reader and is
     * recycled on the next call, so the values are copied out before returning —
     * the same contract a real consumer has to honour.
     */
    private static List<Long> exchangeSums(ClientStreamSession<?> session, long[] a, long[] b) {
        try (VectorSchemaRoot input = VectorSchemaRoot.create(PAIR_INPUT, Allocators.root())) {
            input.allocateNew();
            BigIntVector av = (BigIntVector) input.getVector("a");
            BigIntVector bv = (BigIntVector) input.getVector("b");
            for (int i = 0; i < a.length; i++) {
                av.setSafe(i, a[i]);
                bv.setSafe(i, b[i]);
            }
            av.setValueCount(a.length);
            bv.setValueCount(b.length);
            input.setRowCount(a.length);

            AnnotatedBatch out = session.exchange(new AnnotatedBatch(input, null));
            VectorSchemaRoot root = out.root();
            BigIntVector result = (BigIntVector) root.getVector("result");
            List<Long> values = new ArrayList<>(root.getRowCount());
            for (int i = 0; i < root.getRowCount(); i++) values.add(result.get(i));
            return values;
        }
    }

    /** One exchange turn over {@code scale}, decoding the int64 result column. */
    private static List<Long> exchangeScaled(ClientStreamSession<?> session, long... xs) {
        try (VectorSchemaRoot input = VectorSchemaRoot.create(SCALE_INPUT, Allocators.root())) {
            input.allocateNew();
            BigIntVector xv = (BigIntVector) input.getVector("x");
            for (int i = 0; i < xs.length; i++) xv.setSafe(i, xs[i]);
            xv.setValueCount(xs.length);
            input.setRowCount(xs.length);

            AnnotatedBatch out = session.exchange(new AnnotatedBatch(input, null));
            VectorSchemaRoot root = out.root();
            BigIntVector result = (BigIntVector) root.getVector("result");
            List<Long> values = new ArrayList<>(root.getRowCount());
            for (int i = 0; i < root.getRowCount(); i++) values.add(result.get(i));
            return values;
        }
    }

    /** One exchange turn over {@code shout}, decoding the utf8 result column. */
    private static List<String> exchangeShouts(ClientStreamSession<?> session, String... words) {
        try (VectorSchemaRoot input = VectorSchemaRoot.create(TEXT_INPUT, Allocators.root())) {
            input.allocateNew();
            VarCharVector sv = (VarCharVector) input.getVector("s");
            for (int i = 0; i < words.length; i++) {
                sv.setSafe(i, words[i].getBytes(StandardCharsets.UTF_8));
            }
            sv.setValueCount(words.length);
            input.setRowCount(words.length);

            AnnotatedBatch out = session.exchange(new AnnotatedBatch(input, null));
            VectorSchemaRoot root = out.root();
            VarCharVector result = (VarCharVector) root.getVector("result");
            List<String> values = new ArrayList<>(root.getRowCount());
            for (int i = 0; i < root.getRowCount(); i++) {
                values.add(new String(result.get(i), StandardCharsets.UTF_8));
            }
            return values;
        }
    }
}
