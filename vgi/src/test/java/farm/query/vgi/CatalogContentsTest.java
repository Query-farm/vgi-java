// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.CatalogContentsResponse;
import farm.query.vgi.protocol.CatalogVersionResponse;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The default {@link VgiService#catalog_contents} composition, against a stub
 * service whose per-schema RPCs are scripted: parent-first ordering, items
 * passed through untouched, zero-count kinds skipped, and no transaction.
 */
final class CatalogContentsTest {

    private static final byte[] ATTACH = {1, 2, 3};

    /** A stub catalog: schemas in a deliberately child-first order, and a log of every RPC made. */
    private static final class Stub {
        final List<String> calls = new ArrayList<>();
        final Map<List<String>, Map<String, Long>> counts = new LinkedHashMap<>();

        VgiService service() {
            InvocationHandler handler = (proxy, method, args) -> {
                String name = method.getName();
                switch (name) {
                    case "catalog_contents":
                        return InvocationHandler.invokeDefault(proxy, method, args);
                    case "catalog_version":
                        assertEquals(null, args[1], "catalog_contents takes no transaction");
                        return new CatalogVersionResponse(42L);
                    case "catalog_schemas": {
                        assertEquals(null, args[1], "catalog_contents takes no transaction");
                        List<byte[]> items = new ArrayList<>();
                        for (var e : counts.entrySet()) {
                            items.add(RecordCodec.serializeToBytes(
                                    new SchemaInfo(null, Map.of(), ATTACH, e.getKey(), e.getValue())));
                        }
                        return new ItemsResponse(items);
                    }
                    default:
                        break;
                }
                if (!name.startsWith("catalog_schema_contents_")) {
                    throw new AssertionError("unexpected RPC " + name);
                }
                @SuppressWarnings("unchecked")
                List<String> path = (List<String>) args[1];
                String kind = name.substring("catalog_schema_contents_".length());
                if (kind.equals("functions") || kind.equals("macros")) {
                    kind = kind + ":" + args[2];
                }
                calls.add(String.join(".", path) + "/" + kind);
                return new ItemsResponse(List.of(
                        (String.join(".", path) + "/" + kind).getBytes(StandardCharsets.UTF_8)));
            };
            return (VgiService) Proxy.newProxyInstance(VgiService.class.getClassLoader(),
                    new Class<?>[] {VgiService.class}, handler);
        }
    }

    private static Map<String, Long> zeroExcept(String... nonZero) {
        Map<String, Long> m = new HashMap<>();
        for (String k : List.of("table", "view", "scalar_function", "aggregate_function",
                "table_function", "macro", "index")) {
            m.put(k, 0L);
        }
        for (String k : nonZero) m.put(k, 3L);
        return m;
    }

    private static List<SchemaContents> decode(CatalogContentsResponse r) {
        return r.schemas().stream()
                .map(b -> RecordCodec.deserializeFromBytes(b, SchemaContents.class))
                .toList();
    }

    private static List<String> path(SchemaContents c) {
        return RecordCodec.deserializeFromBytes(c.schema(), SchemaInfo.class).path();
    }

    private static String text(List<byte[]> items) {
        return items.isEmpty() ? "" : new String(items.get(0), StandardCharsets.UTF_8);
    }

    @Test
    void schemasComeBackParentsFirstAndStable() {
        Stub stub = new Stub();
        stub.counts.put(List.of("a", "b", "c"), null);
        stub.counts.put(List.of("z"), null);
        stub.counts.put(List.of("a", "b"), null);
        stub.counts.put(List.of("a"), null);

        CatalogContentsResponse r = stub.service().catalog_contents(ATTACH, null);

        assertEquals(42L, r.catalog_version());
        assertEquals(List.of(List.of("z"), List.of("a"), List.of("a", "b"), List.of("a", "b", "c")),
                decode(r).stream().map(CatalogContentsTest::path).toList());
    }

    @Test
    void itemsArePassedThroughForEveryKindWhenCountsAreUnknown() {
        Stub stub = new Stub();
        stub.counts.put(List.of("s"), null);

        SchemaContents c = decode(stub.service().catalog_contents(ATTACH, null)).get(0);

        assertEquals("s/tables", text(c.tables()));
        assertEquals("s/views", text(c.views()));
        assertEquals("s/functions:SCALAR_FUNCTION", text(c.scalar_functions()));
        assertEquals("s/functions:AGGREGATE_FUNCTION", text(c.aggregate_functions()));
        assertEquals("s/functions:TABLE_FUNCTION", text(c.table_functions()));
        assertEquals("s/macros:SCALAR_MACRO", text(c.scalar_macros()));
        assertEquals("s/macros:TABLE_MACRO", text(c.table_macros()));
        assertEquals("s/indexes", text(c.indexes()));
        assertEquals(8, stub.calls.size());
    }

    @Test
    void kindsWithAZeroCountAreNotListed() {
        Stub stub = new Stub();
        stub.counts.put(List.of("s"), zeroExcept("table_function", "macro"));

        SchemaContents c = decode(stub.service().catalog_contents(ATTACH, null)).get(0);

        assertEquals(List.of("s/functions:TABLE_FUNCTION", "s/macros:SCALAR_MACRO", "s/macros:TABLE_MACRO"),
                stub.calls);
        assertTrue(c.tables().isEmpty() && c.views().isEmpty() && c.scalar_functions().isEmpty()
                && c.aggregate_functions().isEmpty() && c.indexes().isEmpty());
        assertEquals("s/functions:TABLE_FUNCTION", text(c.table_functions()));
    }

    @Test
    void aMissingCountKeyStillLists() {
        Stub stub = new Stub();
        Map<String, Long> partial = new HashMap<>(Map.of("table", 0L));
        stub.counts.put(List.of("s"), partial);

        stub.service().catalog_contents(ATTACH, null);

        assertEquals(7, stub.calls.size(), "only the zero-count kind is skipped: " + stub.calls);
    }

    @Test
    void theSchemaItemIsTheCatalogSchemasItemVerbatim() {
        Stub stub = new Stub();
        stub.counts.put(List.of("s"), zeroExcept());
        VgiService service = stub.service();

        byte[] expected = service.catalog_schemas(ATTACH, null).items().get(0);
        SchemaContents c = decode(service.catalog_contents(ATTACH, null)).get(0);

        assertArrayEquals(expected, c.schema());
    }
}
