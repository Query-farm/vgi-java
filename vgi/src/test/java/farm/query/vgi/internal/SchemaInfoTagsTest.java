// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that per-schema tags declared on the {@link Worker} via
 * {@link Worker#schemaTags(String, Map)} flow through the
 * {@code catalog_schemas} enumeration onto the wire {@link SchemaInfo#tags()}
 * (surfaced by DuckDB as {@code duckdb_schemas().tags}). The default — no tags
 * configured — yields an empty tag map.
 */
class SchemaInfoTagsTest {

    private static VgiServiceImpl service(Worker w) {
        // sealOpaqueData=false ⇒ stdio/AF_UNIX contract: the attach token is the
        // plaintext id, so we can drive catalog_schemas directly with any bytes.
        return new VgiServiceImpl(w, List.of(), List.of(), List.of(), List.of(), false);
    }

    private static SchemaInfo onlySchema(ItemsResponse resp) {
        List<byte[]> items = resp.items();
        assertEquals(1, items.size(), "expected exactly one schema");
        return RecordCodec.deserializeFromBytes(items.get(0), SchemaInfo.class);
    }

    @Test
    void schemaTagsSurfaceOnSchemaInfo() {
        Worker w = Worker.builder()
                .schemaTags("main", Map.of(
                        "vgi.description_llm", "Main schema for testing.",
                        "vgi.description_md", "# Main\nMain schema."));

        SchemaInfo info = onlySchema(service(w).catalog_schemas("attach".getBytes(), null, null));

        assertEquals("main", info.name());
        assertEquals("Main schema for testing.", info.tags().get("vgi.description_llm"));
        assertEquals("# Main\nMain schema.", info.tags().get("vgi.description_md"));
        assertEquals(2, info.tags().size());
    }

    @Test
    void schemaTagsMergeAcrossCalls() {
        Worker w = Worker.builder()
                .schemaTags("main", Map.of("a", "1"))
                .schemaTags("main", Map.of("b", "2"));

        SchemaInfo info = onlySchema(service(w).catalog_schemas("attach".getBytes(), null, null));

        assertEquals("1", info.tags().get("a"));
        assertEquals("2", info.tags().get("b"));
    }

    /** A table function that is only ever enumerated, never bound. */
    private static farm.query.vgi.table.TableFunction tableFunction(String name) {
        return new farm.query.vgi.table.TableFunction() {
            @Override public farm.query.vgi.function.FunctionSpec spec() {
                return farm.query.vgi.function.FunctionSpec.builder(name).build();
            }
            @Override public farm.query.vgi.protocol.BindResponse onBind(farm.query.vgi.table.TableBindParams p) {
                throw new UnsupportedOperationException();
            }
            @Override public farm.query.vgi.table.TableProducerState createProducer(
                    farm.query.vgi.table.TableInitParams p) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * An auxiliary schema the worker gives no comment reports a NULL comment, not "": DuckDB
     * surfaces "" in {@code duckdb_schemas().comment} as a (blank) comment.
     */
    @Test
    void undeclaredSchemaCommentIsNull() {
        Worker w = Worker.builder()
                .schemaComment("data", "Commented")
                .registerTable("data", tableFunction("data_fn"))
                .registerTable("aux", tableFunction("aux_fn"));

        Map<String, String> comments = new java.util.HashMap<>();
        VgiServiceImpl svc = new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates(), false);
        for (byte[] item : svc.catalog_schemas("attach".getBytes(), null, null).items()) {
            SchemaInfo info = RecordCodec.deserializeFromBytes(item, SchemaInfo.class);
            comments.put(info.name(), info.comment());
        }
        assertEquals("Commented", comments.get("data"), comments.toString());
        assertTrue(comments.containsKey("aux"));
        assertNull(comments.get("aux"));
    }

    @Test
    void noTagsConfiguredYieldsEmptyMap() {
        SchemaInfo info = onlySchema(
                service(Worker.builder()).catalog_schemas("attach".getBytes(), null, null));

        assertEquals("main", info.name());
        assertTrue(info.tags().isEmpty(), "tags should default to empty");
    }
}
