// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The partitioned fixtures offer no more readers than they have work items.
 *
 * <p>DuckDB opens {@code min(max_workers, threads)} readers for a scan, from the
 * {@code max_workers} of its init response. A fixed 8 gave a one-chunk call
 * seven readers with nothing to do, each costing an init and an empty drain.
 */
final class PartitionedMaxWorkersTest {

    private static EmbeddedJavaHttpWorker worker;

    @BeforeAll
    static void startWorker() throws Exception {
        worker = EmbeddedJavaHttpWorker.start();
    }

    @AfterAll
    static void stopWorker() {
        if (worker != null) worker.close();
    }

    @Test
    @Timeout(120)
    void maxWorkersIsTheNumberOfWorkItemsCappedAtEight() {
        try (HttpRpcConnection connection = HttpRpcConnection.builder(worker.endpoint()).build()) {
            VgiService vgi = connection.proxy(VgiService.class);
            byte[] handle = vgi.catalog_attach(CatalogAttachRequest.of("example", new byte[0], "", ""), null)
                    .attach_opaque_data();
            // partitioned_sequence: 10,000-row chunks.
            assertEquals(1L, maxWorkers(vgi, handle, "partitioned_sequence", 1L));
            assertEquals(1L, maxWorkers(vgi, handle, "partitioned_sequence", 10_000L));
            assertEquals(2L, maxWorkers(vgi, handle, "partitioned_sequence", 10_001L));
            assertEquals(8L, maxWorkers(vgi, handle, "partitioned_sequence", 1_000_000L));
            // filter_echo_partitioned: 1,000-row chunks.
            assertEquals(1L, maxWorkers(vgi, handle, "filter_echo_partitioned", 100L));
            assertEquals(3L, maxWorkers(vgi, handle, "filter_echo_partitioned", 2_500L));
            assertEquals(8L, maxWorkers(vgi, handle, "filter_echo_partitioned", 200_000L));
        }
    }

    private static long maxWorkers(VgiService vgi, byte[] handle, String function, long count) {
        BindRequest bind = new BindRequest(
                function, ArgumentsEncoder.positionalArgs(count), "TABLE", null,
                SettingsEncoder.builder().encode(), null, handle, null, false,
                null, null, null, null, "main");
        BindResponse bound = vgi.bind(bind, null);
        RpcStream<? extends StreamState> stream = vgi.init(new InitRequest(
                RecordCodec.serializeToBytes(bind), bound.output_schema(), bound.opaque_data(),
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
                null);
        try {
            return ((GlobalInitResponse) stream.header()).max_workers();
        } finally {
            stream.close();
        }
    }
}
