// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code catalog_schema_contents_functions} builds each listing once.
 *
 * <p>A listing derives only from the registered functions' static metadata, but
 * building one binds every function in the schema (with empty arguments, for its
 * output schema) and Arrow-encodes its {@code FunctionInfo}. It was rebuilt on
 * every request -- about 72 ms per main-schema listing of the example worker,
 * paid by every attach that loads a function set.
 */
class FunctionListingCacheTest {

    /** A table function that counts how often a listing binds it. */
    private static final class Counted implements TableFunction {
        private final FunctionSpec spec;
        final AtomicInteger binds = new AtomicInteger();

        Counted(String name) {
            spec = FunctionSpec.builder(name).description(name).build();
        }

        @Override public FunctionSpec spec() { return spec; }

        @Override public BindResponse onBind(TableBindParams params) {
            binds.incrementAndGet();
            return BindResponse.forSchema(SchemaUtil.serializeSchema(
                    Schemas.of(Schemas.nullable("n", Schemas.INT64))));
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            throw new UnsupportedOperationException("listed, never scanned");
        }
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    @Test
    void repeatedListingsBindEachFunctionOnce() {
        Counted mainFn = new Counted("listed_main");
        Counted dataFn = new Counted("listed_data");
        VgiServiceImpl svc = service(Worker.builder()
                .catalogName("listing_catalog")
                .registerTable("main", mainFn)
                .registerTable("data", dataFn));
        byte[] attach = svc.catalog_attach(
                CatalogAttachRequest.of("listing_catalog", null, null, null), null).attach_opaque_data();
        byte[] secondAttach = svc.catalog_attach(
                CatalogAttachRequest.of("listing_catalog", null, null, null), null).attach_opaque_data();

        for (int i = 0; i < 5; i++) {
            assertEquals(List.of("listed_main"), names(svc, attach, "main", "table"));
            assertEquals(List.of("listed_main"), names(svc, secondAttach, "main", "table"));
            assertEquals(List.of("listed_main"), names(svc, attach, "main", null));
        }
        // Once for the item, however many listings and attaches it appears in.
        assertEquals(1, mainFn.binds.get());
        assertEquals(0, dataFn.binds.get(), "listing main must not touch data's functions");

        assertEquals(List.of("listed_data"), names(svc, attach, "data", "table"));
        assertEquals(List.of(), names(svc, attach, "data", "scalar"));
        assertEquals(1, dataFn.binds.get());
    }

    private static List<String> names(VgiServiceImpl svc, byte[] attach, String schema, String type) {
        ItemsResponse items = svc.catalog_schema_contents_functions(attach, List.of(schema), type, null, null);
        return items.items().stream()
                .map(b -> RecordCodec.deserializeFromBytes(b, FunctionInfo.class).name())
                .toList();
    }
}
