// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.View;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.CatalogContentsResponse;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code catalog_contents} through the real client: the attach flag, and the
 * whole-catalog answer agreeing byte for byte with the per-schema RPCs it
 * replaces.
 *
 * <p>The contract is that a client can seed its caches from
 * {@code catalog_contents} and never see a difference from the per-schema path,
 * so every item is compared as bytes, kind by kind, schema by schema — including
 * the kinds a schema has none of, which must come back as empty lists.</p>
 */
final class CatalogContentsRoundTripTest {

    private static Worker worker() {
        return Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerTable(new VgiClientRoundTripTest.SeqFunction())
                .registerScalar(new VgiScalarExchangeRoundTripTest.AddPairFunction())
                // A schema holding nothing but one scalar: every other kind
                // reports a zero count, which catalog_contents must honour.
                .registerScalar("extra", new VgiScalarExchangeRoundTripTest.ShoutFunction())
                .registerView(new View("main", "v_one", "SELECT 1 AS one", "a view"))
                .registerMacro(new Macro("main", "twice", MacroType.SCALAR, List.of("x"), "x * 2", "doubles"))
                .registerMacro(new Macro("main", "nums", MacroType.TABLE, List.of(),
                        "SELECT * FROM range(3)", "three rows"));
    }

    private static CatalogAttachResult attach(VgiService vgi) {
        return vgi.catalog_attach(CatalogAttachRequest.of("testcat", null, null, null), null);
    }

    @Test
    @Timeout(60)
    void attachAdvertisesCatalogContentsByDefault() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            assertTrue(attach(h.client()).supports_catalog_contents());
        }
    }

    @Test
    @Timeout(60)
    void attachDoesNotAdvertiseWhenTurnedOff() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker().supportsCatalogContents(false))) {
            assertFalse(attach(h.client()).supports_catalog_contents());
        }
    }

    @Test
    @Timeout(60)
    void contentsEqualThePerSchemaRpcsByteForByte() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            VgiService vgi = h.client();
            byte[] handle = attach(vgi).attach_opaque_data();

            CatalogContentsResponse response = vgi.catalog_contents(handle, null);
            assertEquals(vgi.catalog_version(handle, null, null).version(), response.catalog_version());

            List<byte[]> schemaItems = vgi.catalog_schemas(handle, null).items();
            assertEquals(schemaItems.size(), response.schemas().size(), "one entry per schema");
            List<String> names = new ArrayList<>();
            for (int i = 0; i < schemaItems.size(); i++) {
                SchemaContents c = RecordCodec.deserializeFromBytes(response.schemas().get(i), SchemaContents.class);
                assertArrayEquals(schemaItems.get(i), c.schema(), "schema item " + i);
                List<String> path = RecordCodec.deserializeFromBytes(c.schema(), SchemaInfo.class).path();
                names.add(String.join(".", path));
                assertSameItems(path + " tables", c.tables(),
                        vgi.catalog_schema_contents_tables(handle, path, null, null));
                assertSameItems(path + " views", c.views(),
                        vgi.catalog_schema_contents_views(handle, path, null));
                assertSameItems(path + " scalar functions", c.scalar_functions(),
                        vgi.catalog_schema_contents_functions(handle, path, "SCALAR_FUNCTION", null, null));
                assertSameItems(path + " aggregate functions", c.aggregate_functions(),
                        vgi.catalog_schema_contents_functions(handle, path, "AGGREGATE_FUNCTION", null, null));
                assertSameItems(path + " table functions", c.table_functions(),
                        vgi.catalog_schema_contents_functions(handle, path, "TABLE_FUNCTION", null, null));
                assertSameItems(path + " scalar macros", c.scalar_macros(),
                        vgi.catalog_schema_contents_macros(handle, path, "SCALAR_MACRO", null));
                assertSameItems(path + " table macros", c.table_macros(),
                        vgi.catalog_schema_contents_macros(handle, path, "TABLE_MACRO", null));
                assertSameItems(path + " indexes", c.indexes(),
                        vgi.catalog_schema_contents_indexes(handle, path, null));
            }
            assertEquals(List.of("main", "extra"), names);

            // Spot-check that the fixture actually exercised every kind it registered.
            Function<Integer, SchemaContents> at = i ->
                    RecordCodec.deserializeFromBytes(response.schemas().get(i), SchemaContents.class);
            SchemaContents main = at.apply(0);
            assertEquals(1, main.table_functions().size());
            assertEquals(1, main.scalar_functions().size());
            assertEquals(1, main.views().size());
            assertEquals(1, main.scalar_macros().size());
            assertEquals(1, main.table_macros().size());
            SchemaContents extra = at.apply(1);
            assertEquals(1, extra.scalar_functions().size());
            assertTrue(extra.table_functions().isEmpty() && extra.views().isEmpty()
                    && extra.scalar_macros().isEmpty() && extra.table_macros().isEmpty()
                    && extra.tables().isEmpty() && extra.indexes().isEmpty());
        }
    }

    private static void assertSameItems(String what, List<byte[]> bulk, ItemsResponse perSchema) {
        assertEquals(perSchema.items().size(), bulk.size(), what + ": item count");
        for (int i = 0; i < bulk.size(); i++) {
            assertArrayEquals(perSchema.items().get(i), bulk.get(i), what + ": item " + i);
        }
    }
}
