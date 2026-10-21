// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.CatalogContents;
import farm.query.vgi.CatalogContentsEtag;
import farm.query.vgi.CatalogContentsProvider;
import farm.query.vgi.CatalogContentsResult;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

            CatalogContentsResponse response = vgi.catalog_contents(handle, null, null);
            assertEquals(vgi.catalog_version(handle, null, null).version(), response.catalog_version());
            assertNull(response.etag(), "no provider, no etag policy: the worker does not revalidate");
            assertFalse(response.not_modified());
            assertFalse(vgi.catalog_contents(handle, "whatever", null).not_modified(),
                    "with no etag, if_none_match is ignored");

            List<byte[]> schemaItems = vgi.catalog_schemas(handle, null, null).items();
            assertEquals(schemaItems.size(), farm.query.vgi.CatalogContents.schemas(response).size(), "one entry per schema");
            List<String> names = new ArrayList<>();
            for (int i = 0; i < schemaItems.size(); i++) {
                SchemaContents c = farm.query.vgi.CatalogContents.schemas(response).get(i);
                assertArrayEquals(schemaItems.get(i), c.schema(), "schema item " + i);
                List<String> path = RecordCodec.deserializeFromBytes(c.schema(), SchemaInfo.class).path();
                assertEquals(path, c.path(), "path equals SchemaInfo.path");
                names.add(String.join(".", path));
                assertSameItems(path + " tables", c.tables(),
                        vgi.catalog_schema_contents_tables(handle, path, null, null));
                assertSameItems(path + " views", c.views(),
                        vgi.catalog_schema_contents_views(handle, path, null, null));
                assertSameItems(path + " scalar functions", c.scalar_functions(),
                        vgi.catalog_schema_contents_functions(handle, path, "SCALAR_FUNCTION", null, null));
                assertSameItems(path + " aggregate functions", c.aggregate_functions(),
                        vgi.catalog_schema_contents_functions(handle, path, "AGGREGATE_FUNCTION", null, null));
                assertSameItems(path + " table functions", c.table_functions(),
                        vgi.catalog_schema_contents_functions(handle, path, "TABLE_FUNCTION", null, null));
                assertSameItems(path + " scalar macros", c.scalar_macros(),
                        vgi.catalog_schema_contents_macros(handle, path, "SCALAR_MACRO", null, null));
                assertSameItems(path + " table macros", c.table_macros(),
                        vgi.catalog_schema_contents_macros(handle, path, "TABLE_MACRO", null, null));
                assertSameItems(path + " indexes", c.indexes(),
                        vgi.catalog_schema_contents_indexes(handle, path, null, null));
            }
            assertEquals(List.of("main", "extra"), names);

            // Spot-check that the fixture actually exercised every kind it registered.
            Function<Integer, SchemaContents> at = i ->
                    farm.query.vgi.CatalogContents.schemas(response).get(i);
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

    @Test
    @Timeout(60)
    void versionEtagRevalidatesOverTheWire() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(
                worker().catalogContents(CatalogContentsProvider.versionEtag()))) {
            VgiService vgi = h.client();
            byte[] handle = attach(vgi).attach_opaque_data();
            long version = vgi.catalog_version(handle, null, null).version();

            CatalogContentsResponse full = vgi.catalog_contents(handle, null, null);
            assertEquals("gen-" + version, full.etag());
            assertFalse(full.not_modified());
            assertEquals(List.of(List.of("main"), List.of("extra")),
                    farm.query.vgi.CatalogContents.schemas(full).stream().map(SchemaContents::path).toList());

            CatalogContentsResponse same = vgi.catalog_contents(handle, full.etag(), null);
            assertTrue(same.not_modified());
            assertEquals(full.etag(), same.etag());
            assertEquals(version, same.catalog_version());
            assertTrue(farm.query.vgi.CatalogContents.schemas(same).isEmpty());

            CatalogContentsResponse other = vgi.catalog_contents(handle, "gen-stale", null);
            assertFalse(other.not_modified());
            assertEquals(farm.query.vgi.CatalogContents.schemas(full).size(), farm.query.vgi.CatalogContents.schemas(other).size());
        }
    }

    @Test
    @Timeout(60)
    void providerSeesTheCatalogAndCanShortCircuit() throws Exception {
        AtomicInteger builds = new AtomicInteger();
        List<String> names = new ArrayList<>();
        CatalogContentsProvider provider = request -> {
            names.add(request.catalogName());
            if ("v1".equals(request.ifNoneMatch())) return CatalogContentsResult.notModified("v1");
            builds.incrementAndGet();
            return CatalogContentsResult.of(request.build(), "v1");
        };
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker().catalogContents(provider).catalogContentsCache(false))) {
            VgiService vgi = h.client();
            byte[] handle = attach(vgi).attach_opaque_data();
            assertEquals(2, farm.query.vgi.CatalogContents.schemas(vgi.catalog_contents(handle, null, null)).size());
            assertTrue(vgi.catalog_contents(handle, "v1", null).not_modified());
            assertEquals(1, builds.get(), "the not_modified answer built nothing");
            assertEquals(List.of("testcat", "testcat"), names);
        }
    }

    @Test
    @Timeout(60)
    void contentHashEtagOverTheWire() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(
                worker().catalogContentsEtag(CatalogContentsEtag.CONTENT_HASH))) {
            VgiService vgi = h.client();
            byte[] handle = attach(vgi).attach_opaque_data();

            CatalogContentsResponse full = vgi.catalog_contents(handle, null, null);
            assertNotNull(full.etag());
            assertEquals(CatalogContents.digest(full), full.etag(),
                    "the etag is the hash of exactly what was sent");
            assertEquals(full.etag(), vgi.catalog_contents(handle, null, null).etag(),
                    "two builds of the same catalog hash alike");

            CatalogContentsResponse same = vgi.catalog_contents(handle, full.etag(), null);
            assertTrue(same.not_modified());
            assertTrue(farm.query.vgi.CatalogContents.schemas(same).isEmpty());
            assertFalse(vgi.catalog_contents(handle, "0".repeat(64), null).not_modified());
        }
    }

    @Test
    @Timeout(60)
    void itemsAreAttachIndependentByDefault() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            VgiService vgi = h.client();
            byte[] first = attach(vgi).attach_opaque_data();
            byte[] second = attach(vgi).attach_opaque_data();
            assertFalse(java.util.Arrays.equals(first, second), "each attach still gets its own envelope");

            SchemaInfo info = RecordCodec.deserializeFromBytes(
                    vgi.catalog_schemas(first, null, null).items().get(0), SchemaInfo.class);
            assertFalse(farm.query.vgirpc.schema.SchemaDerivation.schemaForRecord(SchemaInfo.class).getFields().stream().anyMatch(f -> f.getName().equals("attach_opaque_data")));
            assertSameItems("schemas", vgi.catalog_schemas(first, null, null).items(), vgi.catalog_schemas(second, null, null));

            List<SchemaContents> a = farm.query.vgi.CatalogContents.schemas(vgi.catalog_contents(first, null, null));
            List<SchemaContents> b = farm.query.vgi.CatalogContents.schemas(vgi.catalog_contents(second, null, null));
            assertEquals(CatalogContents.digest(a), CatalogContents.digest(b), "one snapshot for every attach");
        }
    }

    @Test
    @Timeout(60)
    void attachScopedItemsKeepContextOutsideSchemaMetadata() throws Exception {
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker().attachScopedCatalogItems(true))) {
            VgiService vgi = h.client();
            byte[] handle = attach(vgi).attach_opaque_data();
            SchemaInfo info = RecordCodec.deserializeFromBytes(
                    vgi.catalog_schemas(handle, null, null).items().get(0), SchemaInfo.class);
            assertTrue(handle.length > 0);
            assertEquals(info.path(), RecordCodec.deserializeFromBytes(
                    farm.query.vgi.CatalogContents.schemas(vgi.catalog_contents(handle, null, null)).get(0).schema(), SchemaInfo.class).path());
            assertFalse(farm.query.vgirpc.schema.SchemaDerivation.schemaForRecord(SchemaInfo.class).getFields().stream().anyMatch(f -> f.getName().equals("attach_opaque_data")));
            assertFalse(worker().attachScopedCatalogItems(true).catalogContentsCache(),
                    "per-attach items turn the cache off");
        }
    }

    @Test
    @Timeout(60)
    void contentsAreBuiltOncePerCatalogAndVersion() throws Exception {
        AtomicInteger builds = new AtomicInteger();
        CatalogContentsProvider counting = request -> {
            builds.incrementAndGet();
            return CatalogContentsProvider.versionEtag().catalogContents(request);
        };
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker().catalogContents(counting))) {
            VgiService vgi = h.client();
            byte[] first = attach(vgi).attach_opaque_data();
            byte[] second = attach(vgi).attach_opaque_data();
            CatalogContentsResponse full = vgi.catalog_contents(first, null, null);
            assertEquals(2, farm.query.vgi.CatalogContents.schemas(full).size());
            assertEquals(2, farm.query.vgi.CatalogContents.schemas(vgi.catalog_contents(second, null, null)).size());
            assertEquals(2, farm.query.vgi.CatalogContents.schemas(vgi.catalog_contents(first, "stale", null)).size());
            CatalogContentsResponse same = vgi.catalog_contents(second, full.etag(), null);
            assertTrue(same.not_modified());
            assertTrue(farm.query.vgi.CatalogContents.schemas(same).isEmpty());
            assertEquals(1, builds.get(), "built once, then served from the cache");
        }
        builds.set(0);
        try (PipeWorkerHarness h = PipeWorkerHarness.start(
                worker().catalogContents(counting).catalogContentsCache(false))) {
            VgiService vgi = h.client();
            byte[] handle = attach(vgi).attach_opaque_data();
            vgi.catalog_contents(handle, null, null);
            vgi.catalog_contents(handle, null, null);
            assertEquals(2, builds.get(), "no cache: the provider answers every call");
        }
    }

    private static void assertSameItems(String what, List<byte[]> bulk, ItemsResponse perSchema) {
        assertEquals(perSchema.items().size(), bulk.size(), what + ": item count");
        for (int i = 0; i < bulk.size(); i++) {
            assertEquals(metadata(perSchema.items().get(i)), metadata(bulk.get(i)), what + ": item " + i);
        }
    }
    private static String metadata(byte[] bytes) {
        try (var reader = new farm.query.vgirpc.wire.IpcStreamReader(new java.io.ByteArrayInputStream(bytes), farm.query.vgirpc.wire.Allocators.root())) {
            reader.readNextBatch();
            return new com.google.gson.Gson().toJson(farm.query.vgirpc.marshal.Marshalling.decodeRow(reader.root(), reader.dictionaryProvider(), reader.wireSchema()));
        } catch (Exception error) { throw new AssertionError(error); }
    }

}
