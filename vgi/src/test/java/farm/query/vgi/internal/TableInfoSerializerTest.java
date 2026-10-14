// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.protocol.TableInfo;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire-shape coverage for raw required paths in TableInfo's nested IPC metadata.
 * AND-of-OR groups retain a third list level for identifier components.
 */
class TableInfoSerializerTest {

    private static TableInfo tableInfoWithRequiredFilters(List<List<List<String>>> cnf) {
        return new TableInfo(
                null, Map.of(), "filings", List.of("company"), new byte[0],
                List.of(), null,
                Map.of("insert", "changes", "delete", "rows"), false,
                null, null, null, null,
                null, null, null, null,
                cnf);
    }

    @Test
    void requiredFiltersSerializesAsRawPathGroups() throws Exception {
        // CNF: accession_number AND one-of(ticker, cik) — a singleton group
        // plus a genuine OR-group.
        List<List<List<String>>> cnf = List.of(List.of(List.of("accession_number")), List.of(List.of("ticker"), List.of("cik")));
        byte[] wire = TableInfoSerializer.serialize(tableInfoWithRequiredFilters(cnf));

        try (RootAllocator alloc = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(wire), alloc)) {
            assertTrue(reader.loadNextBatch(), "expected one record batch");
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            Field outer = root.getSchema().findField("required_filters");
            assertInstanceOf(ArrowType.List.class, outer.getType(), "outer must be a list");
            Field inner = outer.getChildren().get(0);
            assertInstanceOf(ArrowType.List.class, inner.getType(), "inner element must be a list");
            Field path = inner.getChildren().get(0);
            assertInstanceOf(ArrowType.List.class, path.getType());
            Field leaf = path.getChildren().get(0);
            assertInstanceOf(ArrowType.Utf8.class, leaf.getType(), "leaf must be utf8");

            ListVector vec = (ListVector) root.getVector("required_filters");
            List<?> groups = (List<?>) vec.getObject(0);
            assertEquals(2, groups.size());

            List<?> g0 = (List<?>) groups.get(0);
            assertEquals(List.of("accession_number"), ((List<?>) g0.get(0)).stream().map(Object::toString).toList());

            List<?> g1 = (List<?>) groups.get(1);
            assertEquals(List.of("ticker"), ((List<?>) g1.get(0)).stream().map(Object::toString).toList());
            assertEquals(List.of("cik"), ((List<?>) g1.get(1)).stream().map(Object::toString).toList());
        }
    }

    @Test
    void literalAndNestedPathsSurviveClientDecoding() {
        List<List<List<String>>> paths = List.of(List.of(List.of("a.b"), List.of("a", "b")),
                List.of(List.of("record", "quoted.\"name")));
        TableInfo decoded = TableInfoDecoder.decode(TableInfoSerializer.serialize(tableInfoWithRequiredFilters(paths)));
        assertEquals(paths, decoded.required_filters());
    }

    @Test
    void emptyRequiredFiltersSerializesAsEmptyOuterList() throws Exception {
        byte[] wire = TableInfoSerializer.serialize(tableInfoWithRequiredFilters(List.of()));

        try (RootAllocator alloc = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(wire), alloc)) {
            assertTrue(reader.loadNextBatch(), "expected one record batch");
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            ListVector vec = (ListVector) root.getVector("required_filters");
            assertEquals(0, ((List<?>) vec.getObject(0)).size());
        }
    }

    @Test
    void writeResultModesRoundTripAsMapOfUtf8() {
        TableInfo decoded = TableInfoDecoder.decode(
                TableInfoSerializer.serialize(tableInfoWithRequiredFilters(List.of())));
        assertEquals(Map.of("insert", "changes", "delete", "rows"), decoded.write_result_modes());
    }
}
