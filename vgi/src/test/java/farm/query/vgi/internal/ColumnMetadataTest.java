// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;
import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonParser;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.InMemoryCatalog;
import farm.query.vgi.protocol.*;
import farm.query.vgirpc.marshal.RecordCodec;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
class ColumnMetadataTest {
    @Test
    void createDiscoveryAndDeniedEffectsFollowSharedMetadataCorpus() throws Exception {
        var corpus =
            JsonParser
                .parseString(Files.readString(Path.of("../conformance/catalog-contracts.json")))
                .getAsJsonObject()
                .getAsJsonObject("column_metadata_schemas");
        var worker = Worker.builder().registerCatalog(new InMemoryCatalog("mem"));
        var service = new VgiServiceImpl(
            worker, worker.scalars(), worker.tables(), worker.tableInOuts(), worker.aggregates());
        var attach = service.catalog_attach(CatalogAttachRequest.of("mem", null, null, null), null)
                         .attach_opaque_data();
        for (var group : corpus.entrySet())
            for (var item : group.getValue().getAsJsonObject().entrySet()) {
                var raw = Base64.getDecoder().decode(item.getValue().getAsString());
                var request = RecordCodec.serializeToBytes(new TableCreateRequest(
                    attach, List.of("main"), item.getKey(), raw, "ERROR", List.of(), null));
                if (group.getKey().equals("invalid")) {
                    assertThrows(
                        Exception.class, () -> ColumnMetadata.validateIpc(raw), item.getKey());
                    assertThrows(Exception.class,
                        () -> service.catalog_table_create(request, null), item.getKey());
                } else {
                    service.catalog_table_create(request, null);
                    var infos = service
                                    .catalog_table_get(attach, List.of("main"), item.getKey(), null,
                                        null, null, null)
                                    .items();
                    assertEquals(1, infos.size());
                    try (var allocator = new org.apache.arrow.memory.RootAllocator();
                         var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                             new java.io.ByteArrayInputStream(infos.getFirst()), allocator)) {
                        assertTrue(reader.loadNextBatch());
                        var columns =
                            ((org.apache.arrow.vector.VarBinaryVector) reader.getVectorSchemaRoot()
                                    .getVector("columns"))
                                .get(0);
                        assertArrayEquals(raw, columns);
                        ColumnMetadata.validateIpc(columns);
                    }
                }
            }
        assertEquals(4,
            service.catalog_schema_contents_tables(attach, List.of("main"), null, null)
                .items()
                .size());
    }
}
