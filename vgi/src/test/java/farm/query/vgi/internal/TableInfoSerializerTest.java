// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

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
 * Wire-shape coverage for {@link TableInfo#required_filters()} — the trailing
 * field serialised as {@code list<list<utf8>>} (conjunctive normal form). Mirror
 * of the vgi-python 0.15.0 {@code TableInfo.required_filters} change.
 */
class TableInfoSerializerTest {

    private static TableInfo tableInfoWithRequiredFilters(List<List<String>> cnf) {
        return new TableInfo(
                null, Map.of(), "filings", "company", new byte[0],
                List.of(), List.of(), List.of(), List.of(), List.of(),
                false, false, false, false, false,
                null, null, null, null,
                null, null, null, null,
                cnf);
    }

    @Test
    void requiredFiltersSerializesAsListOfListOfUtf8() throws Exception {
        // CNF: accession_number AND one-of(ticker, cik) — a singleton group
        // plus a genuine OR-group.
        List<List<String>> cnf = List.of(List.of("accession_number"), List.of("ticker", "cik"));
        byte[] wire = TableInfoSerializer.serialize(tableInfoWithRequiredFilters(cnf));

        try (RootAllocator alloc = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(wire), alloc)) {
            assertTrue(reader.loadNextBatch(), "expected one record batch");
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            Field outer = root.getSchema().findField("required_filters");
            assertInstanceOf(ArrowType.List.class, outer.getType(), "outer must be a list");
            Field inner = outer.getChildren().get(0);
            assertInstanceOf(ArrowType.List.class, inner.getType(), "inner element must be a list");
            Field leaf = inner.getChildren().get(0);
            assertInstanceOf(ArrowType.Utf8.class, leaf.getType(), "leaf must be utf8");

            ListVector vec = (ListVector) root.getVector("required_filters");
            List<?> groups = (List<?>) vec.getObject(0);
            assertEquals(2, groups.size());

            List<?> g0 = (List<?>) groups.get(0);
            assertEquals(List.of("accession_number"), g0.stream().map(Object::toString).toList());

            List<?> g1 = (List<?>) groups.get(1);
            assertEquals(List.of("ticker", "cik"), g1.stream().map(Object::toString).toList());
        }
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
}
