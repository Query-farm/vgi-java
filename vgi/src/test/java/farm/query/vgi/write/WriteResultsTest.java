// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.write;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.Stability;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteResultsTest {

    static final Schema TABLE = new Schema(List.of(Schemas.nonNull("id", Schemas.INT32),
            Schemas.nullable("name", Schemas.UTF8)));

    static Map<String, Object> row(int id, String name) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("name", name);
        return m;
    }

    @Test
    void rowIdFieldCarriesTheMarkerAndUserColumnsDropIt() {
        Field rowid = WriteResults.rowIdField();
        assertEquals("rowid", rowid.getName());
        assertEquals(new ArrowType.Int(64, true), rowid.getType());
        assertTrue(WriteResults.isRowId(rowid));
        Schema withRowId = new Schema(List.of(TABLE.getFields().get(0), TABLE.getFields().get(1), rowid));
        assertEquals(TABLE, WriteResults.userColumns(withRowId));
    }

    @Test
    void resultSchemasMatchTheWireShapes() {
        assertEquals("count", WriteResults.resultSchema(WriteResultMode.COUNT, TABLE).getFields().get(0).getName());
        assertFalse(WriteResults.resultSchema(WriteResultMode.COUNT, TABLE).getFields().get(0).isNullable());
        assertEquals(TABLE, WriteResults.resultSchema(WriteResultMode.ROWS, TABLE));
        Schema changes = WriteResults.resultSchema(WriteResultMode.CHANGES, TABLE);
        assertEquals(List.of("old", "new"), changes.getFields().stream().map(Field::getName).toList());
        assertEquals(TABLE.getFields(), changes.getFields().get(0).getChildren());
    }

    @Test
    void resultBatchesPerMode() {
        List<Map<String, Object>> before = Arrays.asList(row(1, "a"), null);
        List<Map<String, Object>> after = Arrays.asList(row(1, "b"), row(2, "c"));
        try (VectorSchemaRoot count = WriteResults.resultBatch(WriteResultMode.COUNT, TABLE, before, after, Allocators.root())) {
            assertEquals(1, count.getRowCount());
            assertEquals(2L, count.getVector(0).getObject(0));
        }
        try (VectorSchemaRoot rows = WriteResults.resultBatch(WriteResultMode.ROWS, TABLE, before, after, Allocators.root())) {
            assertEquals(2, rows.getRowCount());
            assertEquals("b", rows.getVector("name").getObject(0).toString());
            assertEquals(2, rows.getVector("id").getObject(1));
        }
        try (VectorSchemaRoot changes = WriteResults.resultBatch(WriteResultMode.CHANGES, TABLE, before, after, Allocators.root())) {
            assertEquals(2, changes.getRowCount());
            assertFalse(changes.getVector("old").isNull(0));
            assertTrue(changes.getVector("old").isNull(1), "an insert has no before image");
            assertFalse(changes.getVector("new").isNull(1));
        }
        // A delete returns the before image in rows mode.
        try (VectorSchemaRoot rows = WriteResults.resultBatch(WriteResultMode.ROWS, TABLE,
                List.of(row(3, "d")), Arrays.asList((Map<String, Object>) null), Allocators.root())) {
            assertEquals("d", rows.getVector("name").getObject(0).toString());
        }
        assertThrows(IllegalArgumentException.class, () -> WriteResults.changesBatch(TABLE, List.of(), List.of(row(1, "a")),
                Allocators.root()));
    }

    @Test
    void modesAreOrdered() {
        assertTrue(WriteResultMode.CHANGES.supports(WriteResultMode.ROWS));
        assertTrue(WriteResultMode.ROWS.supports(WriteResultMode.COUNT));
        assertFalse(WriteResultMode.COUNT.supports(WriteResultMode.ROWS));
        assertEquals(WriteResultMode.ROWS, WriteResultMode.fromWire("rows"));
        assertEquals(WriteOperation.UPDATE, WriteOperation.fromWire("update"));
        assertThrows(IllegalArgumentException.class, () -> WriteResultMode.fromWire("all"));
    }

    /** The batch the extension's BuildWriteOptions sends. */
    static byte[] writeOptions(String mode, String onConflict, List<String> columns) {
        Schema schema = new Schema(List.of(
                new Field("result_mode", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("on_conflict", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("on_conflict_columns", FieldType.nullable(new ArrowType.List()),
                        List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)))));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            ((VarCharVector) root.getVector(0)).setSafe(0, mode.getBytes(StandardCharsets.UTF_8));
            ((VarCharVector) root.getVector(1)).setSafe(0, onConflict.getBytes(StandardCharsets.UTF_8));
            UnionListWriter w = ((ListVector) root.getVector(2)).getWriter();
            w.setPosition(0);
            w.startList();
            for (String c : columns) {
                byte[] b = c.getBytes(StandardCharsets.UTF_8);
                try (var buf = Allocators.root().buffer(b.length)) {
                    buf.setBytes(0, b);
                    w.varChar().writeVarChar(0, b.length, buf);
                }
            }
            w.endList();
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    @Test
    void writeOptionsDecode() {
        Map<String, Object> named = new HashMap<>();
        named.put(WriteOptions.ARGUMENT_NAME, writeOptions("rows", "nothing", List.of("id")));
        WriteOptions o = WriteOptions.from(new Arguments(List.of(), named));
        assertEquals(WriteResultMode.ROWS, o.resultMode());
        assertEquals("nothing", o.onConflict());
        assertEquals(List.of("id"), o.onConflictColumns());
        assertEquals(WriteOptions.defaults(), WriteOptions.from(new Arguments(List.of(), Map.of())));
        assertEquals(WriteOptions.defaults(), WriteOptions.from(null));
    }

    @Test
    void functionMetadataWithStability() {
        FunctionMetadata m = FunctionMetadata.describe("d").withCategories("c").withStability(Stability.VOLATILE);
        assertEquals(Stability.VOLATILE, m.stability());
        assertEquals(List.of("c"), m.categories());
        assertEquals("d", m.description());
    }
}
