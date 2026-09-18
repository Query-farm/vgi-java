// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpRpcStream;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code dynamic_filter_echo} driven through the real HTTP stack, one turn at a
 * time, with this test playing the DuckDB client: the requests (init, and each
 * tick's {@code vgi_pushdown_filters} delta) are hand-built, every response is
 * the worker's.
 *
 * <p>Pins two defects. A projected scan ended after its first batch: the fixture
 * emitted its full schema, which over HTTP became the response's stream schema,
 * so the continuation-token batch that followed (written against the projected
 * schema) was malformed and the client never read the token. And the cursor
 * carried every tick's delta, so it grew by one delta per tick and each turn
 * replayed all of them.
 */
final class DynamicFilterHttpTurnsTest {

    private static final Field VALUE_0 = new Field("value_0", FieldType.nullable(new ArrowType.Int(64, true)), null);
    private static final Field VALUE_1 = new Field("value_1", FieldType.nullable(new ArrowType.Int(64, true)), null);

    private static EmbeddedJavaHttpWorker worker;
    private HttpRpcConnection connection;
    private VgiService vgi;
    private byte[] handle;
    private RecordingHttpClient requests;

    @BeforeAll
    static void startWorker() throws Exception {
        worker = EmbeddedJavaHttpWorker.start();
    }

    @AfterAll
    static void stopWorker() {
        if (worker != null) worker.close();
    }

    @BeforeEach
    void attach() {
        requests = new RecordingHttpClient();
        connection = HttpRpcConnection.builder(worker.endpoint()).httpClient(requests).build();
        vgi = connection.proxy(VgiService.class);
        handle = vgi.catalog_attach(CatalogAttachRequest.of("example", new byte[0], "", ""), null)
                .attach_opaque_data();
    }

    @AfterEach
    void disconnect() {
        if (connection != null) connection.close();
    }

    @Test
    @Timeout(120)
    void aProjectedScanReadsEveryBatch() {
        // SELECT MIN(n) FROM (SELECT * FROM dynamic_filter_echo(1000) ORDER BY n LIMIT 5)
        // projects n alone. It returned 900 -- the first batch -- and stopped.
        HttpRpcStream<?> stream = open(1000, 100, List.of(0), null);
        List<Long> rows = new ArrayList<>();
        try {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = stream.tick();
                } catch (NoSuchElementException end) {
                    break;
                }
                assertEquals(List.of("n"), fieldNames(batch.root()), "only the projected column may be emitted");
                BigIntVector n = (BigIntVector) batch.root().getVector("n");
                for (int i = 0; i < batch.root().getRowCount(); i++) rows.add(n.get(i));
            }
        } finally {
            stream.close();
        }
        assertEquals(1000, rows.size());
        assertEquals(0L, rows.stream().mapToLong(Long::longValue).min().orElseThrow());
    }

    @Test
    @Timeout(120)
    void theContinuationStopsGrowingAsATopNBoundTightens() {
        // Rows descend from 999 in batches of 10 and every tick narrows n < bound,
        // so every tick's delta changes the live predicate: the case the client
        // cannot skip. Each continuation request carries the cursor the previous
        // turn minted, which used to hold every delta so far.
        int ticks = 60;
        HttpRpcStream<?> stream = open(1000, 10, null, emptySnapshot());
        try {
            assertEquals(10, stream.tick().root().getRowCount());
            List<Long> sizes = new ArrayList<>();
            for (int revision = 1; revision <= ticks; revision++) {
                int bound = 1000 - 10 * revision;
                AnnotatedBatch batch = stream.tick(tick(List.of(VALUE_0), List.of((long) bound),
                        upsert("top_n:0", revision, "lt", 0)));
                assertEquals("PushdownFilters([ConstantFilter(n < " + bound + ")])", echoed(batch));
                assertEquals(bound - 1, ((BigIntVector) batch.root().getVector("n")).get(0));
                sizes.add(requests.lastBodyBytes("/exchange"));
            }
            long early = sizes.get(4);
            long late = sizes.get(ticks - 1);
            assertTrue(late - early < 128, "the continuation request grew with the tick count: " + sizes);
        } finally {
            stream.close();
        }
    }

    @Test
    @Timeout(120)
    void aTurnRebuiltFromTheTokensKeepsThePredicateOrder() {
        // {a:1, b:1} -> [a, b];  {remove a:2, b:1} -> [b];  {a:3, b:1} -> [b, a].
        // A shorter replay yields [a, b]; the next turn must show [b, a]. It is
        // made to rebuild by sending it a delta that changes nothing.
        HttpRpcStream<?> stream = open(1000, 10, null, emptySnapshot());
        try {
            stream.tick();
            stream.tick(tick(List.of(VALUE_0, VALUE_1), List.of(100_000L, 5L),
                    upsert("top_n:0", 1, "lt", 0), upsert("top_n:1", 1, "gt", 1)));
            stream.tick(tick(List.of(VALUE_0), List.of(5L), remove("top_n:0", 2), upsert("top_n:1", 1, "gt", 0)));
            String live = echoed(stream.tick(tick(List.of(VALUE_0, VALUE_1), List.of(99_999L, 5L),
                    upsert("top_n:0", 3, "lt", 0), upsert("top_n:1", 1, "gt", 1))));
            assertTrue(live.indexOf("ConstantFilter(n > 5)") < live.indexOf("ConstantFilter(n < 99999)"), live);
            String rebuilt = echoed(stream.tick(tick(List.of(VALUE_0), List.of(5L), upsert("top_n:1", 1, "gt", 0))));
            assertEquals(live, rebuilt);
        } finally {
            stream.close();
        }
    }

    @Test
    @Timeout(120)
    void aTombstoneSurvivesAcrossTurns() {
        HttpRpcStream<?> stream = open(1000, 10, null, emptySnapshot());
        try {
            stream.tick();
            stream.tick(tick(List.of(VALUE_0), List.of(100_000L), upsert("top_n:0", 1, "lt", 0)));
            assertEquals("(none)", echoed(stream.tick(tick(List.of(), List.of(), remove("top_n:0", 2)))));
            String stale = echoed(stream.tick(tick(List.of(VALUE_0), List.of(5L), upsert("top_n:0", 1, "lt", 0))));
            assertEquals("(none)", stale, "a stale upsert resurrected a removed predicate");
        } finally {
            stream.close();
        }
    }

    // ------------------------------------------------------------------

    private HttpRpcStream<?> open(long count, long batchSize, List<Integer> projection, byte[] filters) {
        BindRequest bind = new BindRequest(
                "dynamic_filter_echo",
                ArgumentsEncoder.builder().positional(count).named("batch_size", batchSize).encode(),
                "TABLE", null, SettingsEncoder.builder().encode(), null,
                handle, null, false, null, null, null, null, "main");
        BindResponse bound = vgi.bind(bind, null);
        InitRequest init = new InitRequest(
                RecordCodec.serializeToBytes(bind), bound.output_schema(), bound.opaque_data(),
                projection, filters, null,
                null, null, null, null, null, null, null, null, null, null, null, null, null);
        return (HttpRpcStream<?>) vgi.init(init, null);
    }

    /**
     * The JDK client {@link HttpRpcConnection} would build, recording each
     * request's path and body size. It observes; every request still reaches
     * the worker and every response is the worker's.
     */
    private static final class RecordingHttpClient extends HttpClient {
        private final HttpClient inner = HttpClient.newBuilder().build();
        private final List<Map.Entry<String, Long>> sent = new CopyOnWriteArrayList<>();

        long lastBodyBytes(String pathSuffix) {
            for (int i = sent.size() - 1; i >= 0; i--) {
                if (sent.get(i).getKey().endsWith(pathSuffix)) return sent.get(i).getValue();
            }
            throw new AssertionError("no request to *" + pathSuffix + " was sent");
        }

        private void record(HttpRequest request) {
            sent.add(Map.entry(request.uri().getPath(),
                    request.bodyPublisher().map(HttpRequest.BodyPublisher::contentLength).orElse(0L)));
        }

        @Override public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
                throws IOException, InterruptedException {
            record(request);
            return inner.send(request, handler);
        }

        @Override public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            record(request);
            return inner.sendAsync(request, handler);
        }

        @Override public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> handler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            record(request);
            return inner.sendAsync(request, handler, pushPromiseHandler);
        }

        @Override public Optional<CookieHandler> cookieHandler() { return inner.cookieHandler(); }
        @Override public Optional<Duration> connectTimeout() { return inner.connectTimeout(); }
        @Override public Redirect followRedirects() { return inner.followRedirects(); }
        @Override public Optional<ProxySelector> proxy() { return inner.proxy(); }
        @Override public SSLContext sslContext() { return inner.sslContext(); }
        @Override public SSLParameters sslParameters() { return inner.sslParameters(); }
        @Override public Optional<Authenticator> authenticator() { return inner.authenticator(); }
        @Override public Version version() { return inner.version(); }
        @Override public Optional<Executor> executor() { return inner.executor(); }
    }

    private static String echoed(AnnotatedBatch batch) {
        VarCharVector v = (VarCharVector) batch.root().getVector("pushed_filters");
        return new String(v.get(0), StandardCharsets.UTF_8);
    }

    private static List<String> fieldNames(VectorSchemaRoot root) {
        return root.getSchema().getFields().stream().map(Field::getName).toList();
    }

    private static byte[] emptySnapshot() {
        return batch(document("snapshot", "predicates", List.of()), List.of(), List.of());
    }

    /** Tick metadata carrying one delta, framed the way the C++ client frames it. */
    private static Map<String, String> tick(List<Field> payloadFields, List<?> payloadValues, String... updates) {
        byte[] delta = batch(document("delta", "updates", List.of(updates)), payloadFields, payloadValues);
        return Map.of("vgi_pushdown_filters", Base64.getEncoder().encodeToString(delta));
    }

    private static String upsert(String id, long revision, String op, int valueRef) {
        return "{\"operation\":\"upsert\",\"id\":\"" + id + "\",\"revision\":" + revision
                + ",\"mode\":\"advisory\",\"source\":\"top_n\",\"expression\":{\"node\":\"comparison\","
                + "\"op\":\"" + op + "\",\"left\":{\"node\":\"column_ref\",\"column_index\":0,"
                + "\"column_name\":\"n\"},\"right\":{\"node\":\"literal\",\"value_ref\":" + valueRef + "}}}";
    }

    private static String remove(String id, long revision) {
        return "{\"operation\":\"remove\",\"id\":\"" + id + "\",\"revision\":" + revision + "}";
    }

    private static String document(String kind, String member, List<String> entries) {
        return "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\""
                + kind + "\",\"" + member + "\":[" + String.join(",", entries) + "]}";
    }

    private static byte[] batch(String json, List<Field> payloadFields, List<?> payloadValues) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("filter_spec", FieldType.notNullable(new ArrowType.Utf8()), null));
        fields.addAll(payloadFields);
        Schema schema = new Schema(fields, Map.of(
                "vgi_filter_encoding", "vgi.filters.v2",
                "vgi_filter_version", "2",
                "vgi_evaluation_context", "vgi.none.v1"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            VectorScalarCodec.write(root.getVector(0), 0, json);
            for (int i = 0; i < payloadValues.size(); i++) {
                VectorScalarCodec.write(root.getVector(i + 1), 0, payloadValues.get(i));
            }
            for (var vector : root.getFieldVectors()) vector.setValueCount(1);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }
}
