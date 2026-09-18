// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpRpcStream;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code dynamic_filter_echo} driven through the real HTTP stack, one turn at a
 * time, with this test playing the DuckDB client: the requests are hand-built,
 * every response is the worker's.
 *
 * <p>A projected scan ended after its first batch: the fixture emitted its full
 * schema, which over HTTP became the response's stream schema, so the
 * continuation-token batch that followed (written against the projected schema)
 * was malformed and the client never read the token.
 */
final class DynamicFilterHttpTurnsTest {

    private static EmbeddedJavaHttpWorker worker;
    private HttpRpcConnection connection;
    private VgiService vgi;
    private byte[] handle;

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
        connection = HttpRpcConnection.builder(worker.endpoint()).build();
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

    private static List<String> fieldNames(VectorSchemaRoot root) {
        return root.getSchema().getFields().stream().map(Field::getName).toList();
    }
}
