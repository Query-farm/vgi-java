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
import java.util.Map;
import org.junit.jupiter.api.Test;
class ColumnExpressionsTest {
    @Test
    void discoveryPreservesCanonicalExpressionsAndUnsupportedCreateHasNoEffects() throws Exception {
        var vectors =
            JsonParser
                .parseString(Files.readString(Path.of("../conformance/catalog-contracts.json")))
                .getAsJsonObject()
                .getAsJsonObject("generated_column_schemas");
        var worker = Worker.builder().registerCatalog(new InMemoryCatalog("mem"));
        var service = new VgiServiceImpl(
            worker, worker.scalars(), worker.tables(), worker.tableInOuts(), worker.aggregates());
        var attach = service.catalog_attach(CatalogAttachRequest.of("mem", null, null, null), null)
                         .attach_opaque_data();
        for (var group : List.of("valid", "invalid"))
            for (var item : vectors.getAsJsonObject(group).entrySet()) {
                var raw = Base64.getDecoder().decode(item.getValue().getAsString());
                var info = new TableInfo(null, Map.of(), item.getKey(), List.of("main"), raw,
                    List.of(), null, Map.of(), false, null, null, null, null, null, null, null,
                    null, List.of());
                if (group.equals("invalid"))
                    assertThrows(
                        Exception.class, () -> TableInfoSerializer.serialize(info), item.getKey());
                else {
                    var encoded = TableInfoSerializer.serialize(info);
                    try (var allocator = new org.apache.arrow.memory.RootAllocator();
                         var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                             new java.io.ByteArrayInputStream(encoded), allocator)) {
                        assertTrue(reader.loadNextBatch());
                        assertArrayEquals(raw,
                            ((org.apache.arrow.vector.VarBinaryVector) reader.getVectorSchemaRoot()
                                    .getVector("columns"))
                                .get(0));
                    }
                }
                var request = RecordCodec.serializeToBytes(new TableCreateRequest(
                    attach, List.of("main"), item.getKey(), raw, "ERROR", List.of(), null));
                if (group.equals("valid") && item.getKey().equals("ordinary"))
                    service.catalog_table_create(request, null);
                else
                    assertThrows(Exception.class,
                        () -> service.catalog_table_create(request, null), item.getKey());
            }
        assertEquals(1,
            service.catalog_schema_contents_tables(attach, List.of("main"), null, null)
                .items()
                .size());
    }
}
