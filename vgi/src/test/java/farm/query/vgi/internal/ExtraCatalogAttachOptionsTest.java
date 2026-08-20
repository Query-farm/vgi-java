// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgi.Worker;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ATTACH options declared by an auxiliary catalog rather than the worker's main
 * one. A worker may serve a catalog that requires an option next to one that
 * takes none, so both the discovery listing and the refusal are per-catalog.
 */
class ExtraCatalogAttachOptionsTest {

    private static final String GATED = "gated";

    private static Worker worker() {
        return Worker.builder()
                .catalogName("host")
                .registerExtraCatalog(new Worker.ExtraCatalog(GATED, null, null, "gated catalog",
                        List.of(
                                AttachOptionSpec.required("api_key", "API key", Schemas.UTF8),
                                AttachOptionSpec.of("region", "Region", Schemas.UTF8, "us-east-1"))));
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    /** A one-row batch whose column names are the supplied option keys. */
    private static byte[] options(String name, String value) {
        Schema schema = Schemas.of(Schemas.nullable(name, Schemas.UTF8));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            ((VarCharVector) root.getVector(name)).setSafe(0, new Text(value));
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    /** Count of {@code attach_option_specs} on a serialized CatalogInfo row. */
    private static int declaredOptionCount(byte[] catalogInfo) {
        return BatchUtil.withReadBatch(catalogInfo, Allocators.root(),
                root -> ((ListVector) root.getVector("attach_option_specs")).getObject(0).size());
    }

    @Test
    void auxiliaryCatalogAdvertisesItsOwnOptionsAtDiscovery() {
        ItemsResponse catalogs = service(worker()).catalog_catalogs();
        assertEquals(2, catalogs.items().size());
        assertEquals(0, declaredOptionCount(catalogs.items().get(0)), "main catalog declares none");
        assertEquals(2, declaredOptionCount(catalogs.items().get(1)));
    }

    @Test
    void omittingARequiredOptionFailsTheAttach() {
        RpcError e = assertThrows(RpcError.class, () -> service(worker())
                .catalog_attach(CatalogAttachRequest.of(GATED, null, null, null), null));
        assertTrue(e.errorMessage().contains("required option 'api_key'"), e.errorMessage());
    }

    @Test
    void supplyingItAttachesNormally() {
        var result = service(worker()).catalog_attach(
                CatalogAttachRequest.of(GATED, options("api_key", "secret"), null, null), null);
        assertTrue(result.attach_opaque_data().length > 0);
    }

    /** The gate belongs to the catalog that declared it: the host catalog
     *  declares no options and still attaches with none supplied. */
    @Test
    void theHostCatalogIsUnaffected() {
        var result = service(worker()).catalog_attach(
                CatalogAttachRequest.of("host", null, null, null), null);
        assertTrue(result.attach_opaque_data().length > 0);
    }
}
