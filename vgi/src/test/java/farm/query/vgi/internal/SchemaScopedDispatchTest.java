// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Dispatch by (schema, function name) — the protocol 1.1.0 contract.
 *
 * <p>A function name is not a unique key: the same name may be registered in
 * two schemas of one catalog, or in two auxiliary catalogs served by the same
 * process. These tests pin that the bind request's {@code schema_name} (and,
 * for the catalog case, the attach) picks the implementation the caller named
 * rather than colliding as an indistinguishable overload.
 */
class SchemaScopedDispatchTest {

    /** Same registered name in every fixture; the output field name identifies
     *  which implementation actually bound. */
    private abstract static class Probe extends ScalarFn {
        @Override public String name() { return "probe"; }

        @Override protected Schema outputSchema(Schema inputSchema, Arguments arguments) {
            return Schemas.of(Schemas.nullable(tag(), Schemas.INT64));
        }

        abstract String tag();
    }

    private static final class MainProbe extends Probe {
        @Override String tag() { return "from_main"; }

        public void compute(@Vector BigIntVector value, BigIntVector result) {
            for (int i = 0; i < value.getValueCount(); i++) result.setSafe(i, 1L);
        }
    }

    private static final class DataProbe extends Probe {
        @Override String tag() { return "from_data"; }

        public void compute(@Vector BigIntVector value, BigIntVector result) {
            for (int i = 0; i < value.getValueCount(); i++) result.setSafe(i, 2L);
        }
    }

    private static final class TwinAProbe extends Probe {
        @Override String tag() { return "from_twin_a"; }

        public void compute(@Vector BigIntVector value, BigIntVector result) {
            for (int i = 0; i < value.getValueCount(); i++) result.setSafe(i, 3L);
        }
    }

    private static final class TwinBProbe extends Probe {
        @Override String tag() { return "from_twin_b"; }

        public void compute(@Vector BigIntVector value, BigIntVector result) {
            for (int i = 0; i < value.getValueCount(); i++) result.setSafe(i, 4L);
        }
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    private static Worker twoSchemas() {
        return Worker.builder()
                .catalogName("probe_catalog")
                .registerScalar("main", new MainProbe())
                .registerScalar("data", new DataProbe());
    }

    private static BindRequest bind(String schemaName, byte[] attach) {
        return new BindRequest("probe", null, "SCALAR", null, null, null,
                attach, null, false, null, null, null, null, schemaName);
    }

    /** The single field name of a bind response's output schema. */
    private static String boundTag(BindResponse resp) {
        Schema s = SchemaUtil.deserializeSchema(resp.output_schema());
        return s.getFields().get(0).getName();
    }

    @Test
    void schemaQualifiedBindPicksThatSchemasImplementation() {
        VgiServiceImpl svc = service(twoSchemas());
        assertEquals("from_main", boundTag(svc.bind(bind("main", null), null)));
        assertEquals("from_data", boundTag(svc.bind(bind("data", null), null)));
    }

    @Test
    void schemaMatchIsCaseInsensitive() {
        VgiServiceImpl svc = service(twoSchemas());
        assertEquals("from_data", boundTag(svc.bind(bind("DATA", null), null)));
    }

    @Test
    void unqualifiedBindResolvesWhenTheNameLivesInOneSchema() {
        // Not every caller can name a schema: a COPY handler is advertised at
        // catalog level, and (until the upstream fix lands) table-in-out binds
        // arrive unqualified. That resolves — as long as it is unambiguous.
        Worker w = Worker.builder().catalogName("probe_catalog").registerScalar(new MainProbe());
        assertEquals("from_main", boundTag(service(w).bind(bind(null, null), null)));
    }

    @Test
    void unqualifiedBindRaisesWhenTheNameSpansTwoSchemas() {
        // No argument signature can tell main.probe from data.probe apart, so
        // picking one silently would be a coin flip. Name both schemas instead.
        VgiServiceImpl svc = service(twoSchemas());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> svc.bind(bind(null, null), null));
        assertTrue(e.getMessage().contains("more than one schema"), e.getMessage());
        assertTrue(e.getMessage().contains("data") && e.getMessage().contains("main"), e.getMessage());
    }

    @Test
    void bindNamingASchemaThatDoesNotDeclareItRaises() {
        // The whole point of carrying the schema: a bind naming `nowhere` must
        // never reach the implementation declared in `main`.
        VgiServiceImpl svc = service(twoSchemas());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> svc.bind(bind("nowhere", null), null));
        assertTrue(e.getMessage().contains("not registered in schema 'nowhere'"), e.getMessage());
        assertTrue(e.getMessage().contains("[data, main]"), e.getMessage());
    }

    @Test
    void bindNamingASchemaOfTheWrongCatalogRaises() {
        // An auxiliary catalog's attach must not reach the main catalog's
        // functions, even for a name only the main catalog declares.
        Worker w = Worker.builder()
                .catalogName("probe_catalog")
                .registerScalar(new MainProbe())
                .registerExtraCatalog(new Worker.ExtraCatalog("aux", "1.0.0", "1.0.0", "aux"));
        VgiServiceImpl svc = service(w);
        byte[] auxAttach = svc.catalog_attach(
                new CatalogAttachRequest("aux", null, null, null), null).attach_opaque_data();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> svc.bind(bind("main", auxAttach), null));
        assertTrue(e.getMessage().contains("not registered in catalog 'aux'"), e.getMessage());
        assertTrue(e.getMessage().contains("probe_catalog"), e.getMessage());
    }

    @Test
    void eachSchemaListsOnlyItsOwnFunction() {
        VgiServiceImpl svc = service(twoSchemas());
        assertEquals(1, listedFunctionCount(svc, null, "main"));
        assertEquals(1, listedFunctionCount(svc, null, "data"));
        assertEquals(0, listedFunctionCount(svc, null, "no_such_schema"));
    }

    @Test
    void attachedCatalogPicksItsOwnImplementation() {
        Worker w = Worker.builder()
                .catalogName("probe_catalog")
                .registerExtraCatalog(new Worker.ExtraCatalog("twin_a", "1.0.0", "1.0.0", "twin a"))
                .registerExtraCatalog(new Worker.ExtraCatalog("twin_b", "1.0.0", "1.0.0", "twin b"))
                .registerExtraCatalogScalar("twin_a", "main", new TwinAProbe())
                .registerExtraCatalogScalar("twin_b", "main", new TwinBProbe());
        VgiServiceImpl svc = service(w);

        // Both catalogs declare main.probe — only the attach tells them apart.
        byte[] attachA = svc.catalog_attach(
                new CatalogAttachRequest("twin_a", null, null, null), null).attach_opaque_data();
        byte[] attachB = svc.catalog_attach(
                new CatalogAttachRequest("twin_b", null, null, null), null).attach_opaque_data();

        assertEquals("from_twin_a", boundTag(svc.bind(bind("main", attachA), null)));
        assertEquals("from_twin_b", boundTag(svc.bind(bind("main", attachB), null)));

        // ...and each catalog advertises only its own.
        assertEquals(1, listedFunctionCount(svc, attachA, "main"));
        assertEquals(1, listedFunctionCount(svc, attachB, "main"));
    }

    @Test
    void mainCatalogHidesFunctionsOwnedByAnAuxiliaryCatalog() {
        Worker w = Worker.builder()
                .catalogName("probe_catalog")
                .registerScalar("main", new MainProbe())
                .registerExtraCatalog(new Worker.ExtraCatalog("twin_a", "1.0.0", "1.0.0", "twin a"))
                .registerExtraCatalogScalar("twin_a", "main", new TwinAProbe());
        VgiServiceImpl svc = service(w);
        CatalogAttachResult mainAttach =
                svc.catalog_attach(new CatalogAttachRequest("probe_catalog", null, null, null), null);
        // One entry, not two: the auxiliary catalog's same-named scalar is not
        // advertised under the main catalog's attach.
        assertEquals(1, listedFunctionCount(svc, mainAttach.attach_opaque_data(), "main"));
    }

    private static int listedFunctionCount(VgiServiceImpl svc, byte[] attach, String schema) {
        ItemsResponse items = svc.catalog_schema_contents_functions(attach, schema, "scalar", null, null);
        return items.items().size();
    }
}
