// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protocol 3.3 {@code FunctionInfo.visibility}: every function a table descriptor names is
 * listed (so the extension always has its capabilities), write functions are always
 * {@code table_only}, a scan is {@code table_only} unless the table sets {@code exposeScan},
 * and {@link Worker#registerUnlistedTable} now means "list as table_only" rather than
 * "omit". Ordinary functions are {@code public}.
 */
class FunctionVisibilityTest {

    private static final byte[] COLUMNS = SchemaUtil.serializeSchema(
            Schemas.of(Schemas.nullable("id", Schemas.INT64)));

    private static final class Scan implements TableFunction {
        private final FunctionSpec spec;
        Scan(String name) { spec = FunctionSpec.builder(name).description(name).build(); }
        @Override public FunctionSpec spec() { return spec; }
        @Override public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(COLUMNS);
        }
        @Override public TableProducerState createProducer(TableInitParams params) {
            throw new UnsupportedOperationException("listed, never scanned");
        }
    }

    private static final class Write implements TableInOutFunction {
        private final String name;
        Write(String name) { this.name = name; }
        @Override public String name() { return name; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("write"); }
        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
        @Override public BindResponse onBind(TableInOutBindParams p) { throw new UnsupportedOperationException(); }
        @Override public TableInOutExchangeState createExchange(TableInOutInitParams p) {
            throw new UnsupportedOperationException();
        }
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    /** Function name to its listed {@code visibility}, for one schema of the worker's catalog. */
    private static Map<String, String> listing(Worker w, String schema) {
        VgiServiceImpl svc = service(w);
        byte[] attach = svc.catalog_attach(
                CatalogAttachRequest.of("vis", null, null, null), null).attach_opaque_data();
        ItemsResponse items = svc.catalog_schema_contents_functions(attach, List.of(schema), null, null, null);
        Map<String, String> out = new TreeMap<>();
        for (byte[] b : items.items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(b, FunctionInfo.class);
            out.put(info.name(), info.visibility());
        }
        return out;
    }

    private static Worker writableWorker(boolean exposeScan) {
        return Worker.builder().catalogName("vis")
                .registerTable("main", new Scan("items_scan"))
                .registerTable("main", new Scan("plain_fn"))
                .registerTableInOut("main", new Write("items_insert"))
                .registerTableInOut("main", new Write("items_update"))
                .registerTableInOut("main", new Write("items_delete"))
                .registerCatalogTable(CatalogTable.builder("main", "items", COLUMNS)
                        .scanFunction("items_scan")
                        .insertFunction("items_insert")
                        .updateFunction("items_update")
                        .deleteFunction("items_delete")
                        .exposeScan(exposeScan)
                        .build());
    }

    @Test
    void tableFunctionsAreListedTableOnlyByDefault() {
        Map<String, String> vis = listing(writableWorker(false), "main");
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("items_scan"));
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("items_insert"));
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("items_update"));
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("items_delete"));
        assertEquals(FunctionInfo.VISIBILITY_PUBLIC, vis.get("plain_fn"));
    }

    @Test
    void exposeScanMakesOnlyTheScanPublic() {
        Map<String, String> vis = listing(writableWorker(true), "main");
        assertEquals(FunctionInfo.VISIBILITY_PUBLIC, vis.get("items_scan"));
        // Write functions are always table_only, whatever the scan flag says.
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("items_insert"));
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("items_update"));
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("items_delete"));
    }

    @Test
    void aScanSharedByTablesIsPublicWhenAnyTableExposesIt() {
        Worker w = Worker.builder().catalogName("vis")
                .registerTable("main", new Scan("shared_scan"))
                .registerCatalogTable(CatalogTable.builder("main", "a", COLUMNS)
                        .scanFunction("shared_scan").build())
                .registerCatalogTable(CatalogTable.builder("main", "b", COLUMNS)
                        .scanFunction("shared_scan").exposeScan(true).build());
        assertEquals(FunctionInfo.VISIBILITY_PUBLIC, listing(w, "main").get("shared_scan"));
    }

    @Test
    void unlistedTableIsNowListedTableOnly() {
        Worker w = Worker.builder().catalogName("vis")
                .registerUnlistedTable(new Scan("hidden_scan"))
                .registerUnlistedTable(new Scan("hidden_exposed"))
                .registerCatalogTable(CatalogTable.builder("main", "t", COLUMNS)
                        .scanFunction("hidden_exposed").exposeScan(true).build());
        Map<String, String> vis = listing(w, "main");
        // Before protocol 3.3 these were omitted, so the extension never saw their capabilities.
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("hidden_scan"));
        // The explicit hiding API wins over exposeScan.
        assertEquals(FunctionInfo.VISIBILITY_TABLE_ONLY, vis.get("hidden_exposed"));
    }

    @Test
    void catalogTableCopiesCarryExposeScan() {
        CatalogTable t = CatalogTable.builder("main", "t", COLUMNS)
                .scanFunction("s").exposeScan(true).build();
        assertTrue(t.exposeScan());
        assertTrue(t.withCardinality(1, 2).exposeScan());
        assertTrue(t.withRpcScanFunction().exposeScan());
        assertTrue(t.withStatistics(List.of()).exposeScan());
        assertTrue(t.withRequiredFilters(List.of()).exposeScan());
        assertTrue(t.withCanonicalConstraints(List.of(), null).exposeScan());
        assertTrue(t.withWriteFunctions(null, null, null, Map.of()).exposeScan());
        assertFalse(CatalogTable.builder("main", "t", COLUMNS).scanFunction("s").build().exposeScan());
        assertFalse(CatalogTable.functionBacked("main", "t", COLUMNS, "", "s").exposeScan());
        assertTrue(CatalogTable.functionBacked("main", "t", COLUMNS, "", "s").withExposeScan(true).exposeScan());
        assertFalse(t.withExposeScan(false).exposeScan());
    }

    @Test
    void visibilityIsAPlainNullableUtf8Column() throws Exception {
        VgiServiceImpl svc = service(writableWorker(false));
        byte[] attach = svc.catalog_attach(
                CatalogAttachRequest.of("vis", null, null, null), null).attach_opaque_data();
        byte[] item = svc.catalog_schema_contents_functions(attach, List.of("main"), "table", null, null)
                .items().get(0);
        try (var alloc = new org.apache.arrow.memory.RootAllocator();
             var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                     new java.io.ByteArrayInputStream(item), alloc)) {
            var field = reader.getVectorSchemaRoot().getSchema().findField("visibility");
            assertTrue(field.isNullable());
            assertNull(field.getDictionary(), "visibility is a plain string, not a dictionary");
            assertEquals(org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE, field.getType());
        }
    }

    @Test
    void withVisibilityReplacesOnlyVisibility() {
        Map<String, String> before = listing(writableWorker(false), "main");
        assertEquals(5, before.size());
        VgiServiceImpl svc = service(writableWorker(false));
        byte[] attach = svc.catalog_attach(
                CatalogAttachRequest.of("vis", null, null, null), null).attach_opaque_data();
        FunctionInfo info = RecordCodec.deserializeFromBytes(svc.catalog_schema_contents_functions(
                attach, List.of("main"), "table", null, null).items().get(0), FunctionInfo.class);
        FunctionInfo cleared = info.withVisibility(null);
        assertNull(cleared.visibility());
        assertEquals(info.name(), cleared.name());
        assertEquals(info.projection_pushdown(), cleared.projection_pushdown());
        assertEquals(info.has_finalize(), cleared.has_finalize());
        assertEquals(info.required_secrets(), cleared.required_secrets());
    }
}
