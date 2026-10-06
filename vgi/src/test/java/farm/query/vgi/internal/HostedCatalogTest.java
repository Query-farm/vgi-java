// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.CatalogContentsEtag;
import farm.query.vgi.CatalogContentsProvider;
import farm.query.vgi.CatalogContentsResult;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.InMemoryCatalog;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.OnConflict;
import farm.query.vgi.catalog.View;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.CatalogContentsResponse;
import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableCreateRequest;
import farm.query.vgi.protocol.ViewInfo;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Catalogs implemented in code ({@link Worker#registerCatalog}, {@link InMemoryCatalog})
 * and the per-catalog {@code catalog_contents} configuration of auxiliary catalogs,
 * driven through the service exactly as the RPC layer drives it.
 */
final class HostedCatalogTest {

    private static final List<String> MAIN = List.of("main");

    /** A scalar of the main catalog. */
    private static final class Probe extends farm.query.vgi.scalar.ScalarFn {
        @Override public String name() { return "probe"; }

        @Override protected org.apache.arrow.vector.types.pojo.Schema outputSchema(
                org.apache.arrow.vector.types.pojo.Schema inputSchema, farm.query.vgi.function.Arguments arguments) {
            return Schemas.of(Schemas.nullable("p", Schemas.INT64));
        }

        public void compute(@farm.query.vgi.scalar.Vector org.apache.arrow.vector.BigIntVector value,
                            org.apache.arrow.vector.BigIntVector result) {
            for (int i = 0; i < value.getValueCount(); i++) result.setSafe(i, 1L);
        }
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    private static byte[] attach(VgiServiceImpl svc, String name) {
        return svc.catalog_attach(CatalogAttachRequest.of(name, null, null, null), null).attach_opaque_data();
    }

    private static List<String> viewNames(VgiServiceImpl svc, byte[] attach, List<String> path) {
        return svc.catalog_schema_contents_views(attach, path, null).items().stream()
                .map(b -> RecordCodec.deserializeFromBytes(b, ViewInfo.class).name()).toList();
    }

    private static List<List<String>> schemaPaths(VgiServiceImpl svc, byte[] attach) {
        return svc.catalog_schemas(attach, null).items().stream()
                .map(b -> RecordCodec.deserializeFromBytes(b, SchemaInfo.class).path()).toList();
    }

    private static long version(VgiServiceImpl svc, byte[] attach) {
        return svc.catalog_version(attach, null, null).version();
    }

    private static byte[] tableCreate(byte[] attach, String name, String onConflict) {
        byte[] columns = SchemaUtil.serializeSchema(Schemas.of(
                Schemas.nullable("a", Schemas.INT32), Schemas.nullable("b", Schemas.UTF8)));
        return RecordCodec.serializeToBytes(new TableCreateRequest(attach, MAIN, name, columns, onConflict,
                List.of(0), List.of(), List.of(), List.of(), List.of(), null));
    }

    /** A generation-counter etag, like vgi-python's contents_reval. */
    private static final class Reval extends InMemoryCatalog {
        Reval() { super("reval"); }

        @Override
        public CatalogContentsResult catalogContents(CatalogContentsProvider.Request request) {
            return CatalogContentsProvider.versionEtag().catalogContents(request);
        }
    }

    private static Worker hostingWorker() {
        return Worker.builder().catalogName("main_catalog")
                .registerCatalog(new InMemoryCatalog("mem"))
                .registerCatalog(new Reval())
                .registerCatalog(new InMemoryCatalog("hash") {
                    @Override
                    public CatalogContentsEtag catalogContentsEtag() { return CatalogContentsEtag.CONTENT_HASH; }
                })
                .registerCatalog(new InMemoryCatalog("zero") {
                    @Override
                    public long version(byte[] attachId) {
                        super.version(attachId);
                        return 0;
                    }
                });
    }

    @Test
    void catalogsListsEveryHostedCatalog() {
        VgiServiceImpl svc = service(hostingWorker());
        assertEquals(5, svc.catalog_catalogs().items().size());
    }

    @Test
    void attachAdvertisesADdlCatalogThatIsNotFrozen() {
        VgiServiceImpl svc = service(hostingWorker());
        CatalogAttachResult r = svc.catalog_attach(CatalogAttachRequest.of("mem", null, null, null), null);
        assertTrue(r.supports_catalog_contents());
        assertFalse(r.catalog_version_frozen());
        assertFalse(r.supports_transactions());
        assertEquals(1L, r.catalog_version());
        assertEquals("main", r.default_schema());
        assertEquals(List.of(MAIN), schemaPaths(svc, r.attach_opaque_data()));
    }

    @Test
    void ddlChangesTheCatalogAndBumpsItsVersion() {
        VgiServiceImpl svc = service(hostingWorker());
        byte[] a = attach(svc, "mem");
        svc.catalog_view_create(a, MAIN, "v1", "SELECT 1 AS x", "ERROR", null, null);
        assertEquals(List.of("v1"), viewNames(svc, a, MAIN));
        assertEquals(2L, version(svc, a));

        svc.catalog_table_create(tableCreate(a, "t1", "ERROR"), null);
        assertEquals(1, svc.catalog_schema_contents_tables(a, MAIN, null, null).items().size());
        assertEquals(1, svc.catalog_table_get(a, MAIN, "t1", null, null, null, null).items().size());
        assertEquals(0, svc.catalog_table_get(a, MAIN, "nope", null, null, null, null).items().size());
        assertEquals(3L, version(svc, a));

        svc.catalog_schema_create(a, List.of("s2"), "ERROR", "second", Map.of(), null, null);
        assertEquals(List.of(MAIN, List.of("s2")), schemaPaths(svc, a));

        svc.catalog_view_drop(a, MAIN, "v1", false, false, null, null);
        svc.catalog_table_drop(a, MAIN, "t1", false, false, null, null);
        svc.catalog_schema_drop(a, List.of("s2"), false, false, null, null);
        assertEquals(List.of(), viewNames(svc, a, MAIN));
        assertEquals(List.of(MAIN), schemaPaths(svc, a));
        assertEquals(7L, version(svc, a));
    }

    @Test
    void onConflictAndNotFoundFollowTheRequest() {
        VgiServiceImpl svc = service(hostingWorker());
        byte[] a = attach(svc, "mem");
        svc.catalog_view_create(a, MAIN, "v", "SELECT 1", "ERROR", null, null);
        assertThrows(IllegalArgumentException.class,
                () -> svc.catalog_view_create(a, MAIN, "v", "SELECT 2", "ERROR", null, null));
        svc.catalog_view_create(a, MAIN, "v", "SELECT 3", "IGNORE", null, null);
        assertEquals("SELECT 1", RecordCodec.deserializeFromBytes(
                svc.catalog_view_get(a, MAIN, "v", null).items().get(0), ViewInfo.class).definition());
        svc.catalog_view_create(a, MAIN, "v", "SELECT 4", "REPLACE", null, null);
        assertEquals("SELECT 4", RecordCodec.deserializeFromBytes(
                svc.catalog_view_get(a, MAIN, "v", null).items().get(0), ViewInfo.class).definition());

        assertThrows(IllegalArgumentException.class,
                () -> svc.catalog_table_drop(a, MAIN, "missing", false, false, null, null));
        svc.catalog_table_drop(a, MAIN, "missing", true, false, null, null);
        assertThrows(IllegalArgumentException.class,
                () -> svc.catalog_view_create(a, List.of("nowhere"), "v", "SELECT 1", "ERROR", null, null));
        // A non-empty schema needs CASCADE.
        svc.catalog_schema_create(a, List.of("s"), "ERROR", null, null, null, null);
        svc.catalog_view_create(a, List.of("s"), "w", "SELECT 1", "ERROR", null, null);
        assertThrows(IllegalArgumentException.class,
                () -> svc.catalog_schema_drop(a, List.of("s"), false, false, null, null));
        svc.catalog_schema_drop(a, List.of("s"), false, true, null, null);
        assertEquals(List.of(MAIN), schemaPaths(svc, a));
    }

    @Test
    void everyAttachIsPrivateAndDetachDiscardsIt() {
        InMemoryCatalog mem = new InMemoryCatalog("mem");
        VgiServiceImpl svc = service(Worker.builder().catalogName("x").registerCatalog(mem));
        byte[] a = attach(svc, "mem");
        byte[] b = attach(svc, "mem");
        svc.catalog_view_create(a, MAIN, "only_a", "SELECT 1", "ERROR", null, null);
        assertEquals(List.of("only_a"), viewNames(svc, a, MAIN));
        assertEquals(List.of(), viewNames(svc, b, MAIN));
        assertEquals(1L, version(svc, b));
        assertEquals(2, mem.attachedCount());

        svc.catalog_detach(a);
        assertEquals(1, mem.attachedCount());
    }

    @Test
    void sharedScopeIsOneCatalogForEveryAttach() {
        VgiServiceImpl svc = service(Worker.builder().catalogName("x")
                .registerCatalog(new InMemoryCatalog("shared", InMemoryCatalog.Scope.SHARED, "c")));
        byte[] a = attach(svc, "shared");
        byte[] b = attach(svc, "shared");
        svc.catalog_view_create(a, MAIN, "v", "SELECT 1", "ERROR", null, null);
        assertEquals(List.of("v"), viewNames(svc, b, MAIN));
    }

    @Test
    void schemaInfoCountsLetTheClientSkipEmptyKinds() {
        VgiServiceImpl svc = service(hostingWorker());
        byte[] a = attach(svc, "mem");
        svc.catalog_view_create(a, MAIN, "v", "SELECT 1", "ERROR", null, null);
        SchemaInfo info = RecordCodec.deserializeFromBytes(
                svc.catalog_schema_get(a, MAIN, null).items().get(0), SchemaInfo.class);
        assertEquals(1L, info.estimated_object_count().get("view"));
        assertEquals(0L, info.estimated_object_count().get("table"));
        assertEquals(0L, info.estimated_object_count().get("scalar_function"));
        assertEquals(0, svc.catalog_schema_get(a, List.of("nope"), null).items().size());
    }

    @Test
    void catalogContentsComposesTheHostedListings() {
        VgiServiceImpl svc = service(hostingWorker());
        byte[] a = attach(svc, "mem");
        svc.catalog_view_create(a, MAIN, "v", "SELECT 1", "ERROR", null, null);
        CatalogContentsResponse r = svc.catalog_contents(a, null, null);
        assertNull(r.etag());
        assertEquals(2L, r.catalog_version());
        SchemaContents main = r.schemas().get(0);
        assertEquals(MAIN, main.path());
        assertEquals(1, main.views().size());
        assertEquals(0, main.tables().size());
    }

    @Test
    void generationEtagAnswersNotModifiedUntilDdl() {
        VgiServiceImpl svc = service(hostingWorker());
        byte[] a = attach(svc, "reval");
        CatalogContentsResponse first = svc.catalog_contents(a, null, null);
        assertEquals("gen-1", first.etag());
        CatalogContentsResponse same = svc.catalog_contents(a, "gen-1", null);
        assertTrue(same.not_modified());
        assertEquals(0, same.schemas().size());

        svc.catalog_view_create(a, MAIN, "v", "SELECT 1", "ERROR", null, null);
        CatalogContentsResponse changed = svc.catalog_contents(a, "gen-1", null);
        assertFalse(changed.not_modified());
        assertEquals("gen-2", changed.etag());
        assertEquals(1, changed.schemas().get(0).views().size());
    }

    @Test
    void contentHashEtagTracksTheContents() {
        VgiServiceImpl svc = service(hostingWorker());
        byte[] a = attach(svc, "hash");
        String etag = svc.catalog_contents(a, null, null).etag();
        assertTrue(etag.matches("[0-9a-f]{64}"), etag);
        assertTrue(svc.catalog_contents(a, etag, null).not_modified());

        svc.catalog_view_create(a, MAIN, "v", "SELECT 1", "ERROR", null, null);
        CatalogContentsResponse changed = svc.catalog_contents(a, etag, null);
        assertFalse(changed.not_modified());
        assertFalse(etag.equals(changed.etag()));
    }

    @Test
    void aCatalogMayReportVersionZero() {
        VgiServiceImpl svc = service(hostingWorker());
        CatalogAttachResult r = svc.catalog_attach(CatalogAttachRequest.of("zero", null, null, null), null);
        assertEquals(0L, r.catalog_version());
        svc.catalog_view_create(r.attach_opaque_data(), MAIN, "v", "SELECT 1", "ERROR", null, null);
        assertEquals(0L, version(svc, r.attach_opaque_data()));
        assertNull(svc.catalog_contents(r.attach_opaque_data(), null, null).etag());
    }

    @Test
    void ddlOnADeclarativeCatalogIsRefused() {
        VgiServiceImpl svc = service(hostingWorker());
        byte[] main = attach(svc, "main_catalog");
        assertThrows(UnsupportedOperationException.class,
                () -> svc.catalog_view_create(main, MAIN, "v", "SELECT 1", "ERROR", null, null));
        assertThrows(UnsupportedOperationException.class,
                () -> svc.catalog_table_create(tableCreate(main, "t", "ERROR"), null));
    }

    @Test
    void aHostedCatalogDeclaresNoFunctionsAndRefusesBinds() {
        VgiServiceImpl svc = service(hostingWorker());
        byte[] a = attach(svc, "mem");
        assertEquals(0, svc.catalog_schema_contents_functions(a, MAIN, null, null, null).items().size());
        assertEquals(0, svc.catalog_copy_from_formats(a, null).items().size());
        // `probe` is the main catalog's; the hosted attach must not reach it.
        Worker w = hostingWorker().registerScalar(new Probe());
        VgiServiceImpl withScalar = service(w);
        byte[] hosted = attach(withScalar, "mem");
        BindRequest bind = new BindRequest("probe", null, "SCALAR", null, null, null,
                hosted, null, false, null, null, null, null, "main");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> withScalar.bind(bind, null));
        assertTrue(e.getMessage().contains("not registered in catalog 'mem'"), e.getMessage());
    }

    @Test
    void onConflictParsesTheWireValue() {
        assertEquals(OnConflict.ERROR, OnConflict.fromWire(null));
        assertEquals(OnConflict.IGNORE, OnConflict.fromWire("ignore"));
        assertEquals(OnConflict.REPLACE, OnConflict.fromWire("REPLACE"));
        assertThrows(IllegalArgumentException.class, () -> OnConflict.fromWire("merge"));
    }

    @Test
    void registeringATakenNameIsRefused() {
        Worker w = Worker.builder().catalogName("taken").registerCatalog(new InMemoryCatalog("mem"));
        assertThrows(IllegalArgumentException.class, () -> w.registerCatalog(new InMemoryCatalog("taken")));
        assertThrows(IllegalArgumentException.class, () -> w.registerCatalog(new InMemoryCatalog("mem")));
    }

    // ------------------------------------------------------------ auxiliary catalogs

    private static Worker auxiliary(boolean advertise, CatalogContentsProvider provider) {
        byte[] cols = SchemaUtil.serializeSchema(Schemas.of(Schemas.nullable("n", Schemas.INT64)));
        return Worker.builder().catalogName("main_catalog")
                .registerExtraCatalog(new Worker.ExtraCatalog("aux", null, null, "main comment")
                        .withCatalogComment("the catalog")
                        .withSchemaComment("extra", "second schema")
                        .withVersionFrozen(true)
                        .withCatalogContents(advertise, provider, CatalogContentsEtag.NONE))
                .registerExtraCatalogTable("aux", CatalogTable.builder("extra", "five", cols)
                        .scanFunction("sequence", List.of(5L), Map.of()).build())
                .registerExtraCatalogView("aux", new View("main", "answer", "SELECT 42 AS answer", "One row"))
                .registerExtraCatalogMacro("aux", new Macro("main", "triple", MacroType.SCALAR, List.of("x"),
                        Map.of(), Map.of(), "x * 3", "Triple", Map.of()))
                .registerExtraCatalogMacro("aux", new Macro("main", "rng", MacroType.TABLE, List.of("n"),
                        Map.of(), Map.of(), "SELECT * FROM range(n)", "Range", Map.of()));
    }

    @Test
    void auxiliaryCatalogServesItsOwnSchemasViewsAndMacros() {
        VgiServiceImpl svc = service(auxiliary(true, null));
        CatalogAttachResult r = svc.catalog_attach(CatalogAttachRequest.of("aux", null, null, null), null);
        assertTrue(r.catalog_version_frozen());
        assertEquals("the catalog", r.comment());
        byte[] a = r.attach_opaque_data();
        assertEquals(List.of(MAIN, List.of("extra")), schemaPaths(svc, a));
        assertEquals("second schema", RecordCodec.deserializeFromBytes(
                svc.catalog_schema_get(a, List.of("extra"), null).items().get(0), SchemaInfo.class).comment());
        assertEquals(List.of("answer"), viewNames(svc, a, MAIN));
        assertEquals(1, svc.catalog_view_get(a, MAIN, "answer", null).items().size());
        assertEquals(1, svc.catalog_schema_contents_macros(a, MAIN, "SCALAR_MACRO", null).items().size());
        assertEquals(1, svc.catalog_schema_contents_macros(a, MAIN, "TABLE_MACRO", null).items().size());
        assertEquals(1, svc.catalog_macro_get(a, MAIN, "rng", null).items().size());

        SchemaContents mainContents = svc.catalog_contents(a, null, null).schemas().get(0);
        assertEquals(1, mainContents.views().size());
        assertEquals(1, mainContents.scalar_macros().size());
        assertEquals(1, mainContents.table_macros().size());
        assertEquals(0, mainContents.tables().size());

        // The main catalog lists none of it.
        byte[] main = attach(svc, "main_catalog");
        assertEquals(List.of(), viewNames(svc, main, MAIN));
    }

    @Test
    void auxiliaryCatalogMayDeclineToAdvertiseCatalogContents() {
        VgiServiceImpl svc = service(auxiliary(false, null));
        assertFalse(svc.catalog_attach(CatalogAttachRequest.of("aux", null, null, null), null)
                .supports_catalog_contents());
    }

    @Test
    void auxiliaryCatalogsFailingProviderSurfaces() {
        VgiServiceImpl svc = service(auxiliary(true, request -> {
            throw new IllegalStateException("deliberately fails");
        }));
        byte[] a = attach(svc, "aux");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> svc.catalog_contents(a, null, null));
        assertEquals("deliberately fails", e.getMessage());
        // The per-schema RPCs still serve the whole catalog.
        assertEquals(2, schemaPaths(svc, a).size());
    }
}
