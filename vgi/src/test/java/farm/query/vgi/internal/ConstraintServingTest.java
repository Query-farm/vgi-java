// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.CatalogInterface;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.catalog.ConstraintRecord.Enforcement;
import farm.query.vgi.catalog.ConstraintRecord.Validation;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.ClientCapabilities;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Constraint records are negotiated on every catalog path without provider code, and a table
 * the extension would reject at ATTACH is refused when served, naming the table and column.
 */
final class ConstraintServingTest {

    private static final String READ = ConstraintRecord.CONSTRAINT_RECORDS_READ;
    private static final String SCALAR = ConstraintRecord.SCALAR_EXPRESSION_READ;
    private static final byte[] REVISION = "rev1".getBytes(StandardCharsets.UTF_8);

    private static byte[] client(String... read) {
        byte[] catalog = RecordCodec.serializeToBytes(
                new CatalogCapabilities(List.of(read), List.of(), List.of(), List.of()));
        return RecordCodec.serializeToBytes(new ClientCapabilities("duckdb", List.of(), List.of(), true,
                List.of(), catalog));
    }

    private static CatalogAttachRequest request(String name, byte[] client) {
        return new CatalogAttachRequest(name, null, null, null, client);
    }

    private static CatalogCapabilities caps(byte[] bytes) {
        return bytes == null ? null : RecordCodec.deserializeFromBytes(bytes, CatalogCapabilities.class);
    }

    private static byte[] columns(Field... fields) {
        return SchemaUtil.serializeSchema(Schemas.of(fields));
    }

    private static ConstraintRecord fact(String schema, String table, String id, String... path) {
        return ConstraintRecord.builder(schema, table, id).notNull(path)
                .enforcement(Enforcement.ENFORCED).validation(Validation.ALL_ROWS).build();
    }

    private static CatalogTable constrained(String schema, String name) {
        return CatalogTable.builder(schema, name, columns(Schemas.nonNull("id", Schemas.INT64),
                        Schemas.nullable("v", Schemas.UTF8)))
                .constraints(List.of(fact(schema, name, "id_nn", "id")), REVISION).build();
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    private static void assertNegotiated(CatalogAttachResult r) {
        assertEquals(List.of(READ, SCALAR), caps(r.catalog_capabilities()).read());
        assertEquals(List.of(READ, SCALAR), caps(r.selected_capabilities()).read());
    }

    // ---- negotiation -------------------------------------------------------------------

    @Test
    void mainCatalogNegotiatesWithoutProviderCode() {
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalogTable(constrained("main", "t")));
        assertNegotiated(svc.catalog_attach(request("cat", client(READ, SCALAR, "vgi.unknown.v1")), null));
        // Offered without CHECK expressions: records only.
        CatalogAttachResult partial = svc.catalog_attach(request("cat", client(READ)), null);
        assertEquals(List.of(READ), caps(partial.selected_capabilities()).read());
        // Scalar expressions are never selected without records.
        CatalogAttachResult scalarOnly = svc.catalog_attach(request("cat", client(SCALAR)), null);
        assertEquals(List.of(), caps(scalarOnly.selected_capabilities()).read());
        // A client offering nothing selects nothing but still sees the advertisement.
        CatalogAttachResult none = svc.catalog_attach(request("cat", null), null);
        assertEquals(List.of(READ, SCALAR), caps(none.catalog_capabilities()).read());
        assertEquals(List.of(), caps(none.selected_capabilities()).read());
    }

    @Test
    void catalogsWithoutRecordsAdvertiseNothing() {
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalogTable(
                CatalogTable.builder("main", "t", columns(Schemas.nullable("v", Schemas.UTF8))).build()));
        CatalogAttachResult r = svc.catalog_attach(request("cat", client(READ, SCALAR)), null);
        assertNull(r.catalog_capabilities());
        assertNull(r.selected_capabilities());
    }

    @Test
    void explicitProviderDecisionIsMergedNotReplaced() {
        Worker w = Worker.builder().catalogName("cat").registerCatalogTable(constrained("main", "t"))
                .catalogCapabilities(req -> new Worker.CatalogCapabilitySelection(
                        RecordCodec.serializeToBytes(new CatalogCapabilities(List.of("vgi.table.options.read.v1"),
                                List.of(), List.of(), List.of())),
                        RecordCodec.serializeToBytes(new CatalogCapabilities(List.of("vgi.table.options.read.v1"),
                                List.of(), List.of(), List.of()))));
        CatalogAttachResult r = service(w).catalog_attach(request("cat", client(READ, SCALAR)), null);
        assertEquals(List.of(READ, SCALAR, "vgi.table.options.read.v1"), caps(r.catalog_capabilities()).read());
        assertEquals(List.of(READ, SCALAR, "vgi.table.options.read.v1"), caps(r.selected_capabilities()).read());
    }

    @Test
    void extraCatalogNegotiatesFromItsOwnTables() {
        Worker w = Worker.builder().catalogName("cat")
                .registerExtraCatalog(new Worker.ExtraCatalog("aux", null, null, "aux catalog"))
                .registerExtraCatalog(new Worker.ExtraCatalog("plain", null, null, "plain catalog"))
                .registerExtraCatalogTable("aux", constrained("main", "t"));
        VgiServiceImpl svc = service(w);
        CatalogAttachResult aux = svc.catalog_attach(request("aux", client(READ, SCALAR)), null);
        assertNegotiated(aux);
        byte[] served = svc.catalog_schema_contents_tables(aux.attach_opaque_data(), List.of("main"), null, null)
                .items().get(0);
        assertEquals(1, farm.query.vgi.client.TableInfoDecoder.decode(served).constraints().size());
        assertNull(svc.catalog_attach(request("plain", client(READ, SCALAR)), null).catalog_capabilities());
        // The main catalog serves no records, so it advertises nothing either.
        assertNull(svc.catalog_attach(request("cat", client(READ, SCALAR)), null).catalog_capabilities());
    }

    /** A code-implemented catalog serving one fixed table. */
    private static final class Fixed implements CatalogInterface {
        private final TableInfo table;

        Fixed(TableInfo table) { this.table = table; }

        @Override public String name() { return "hosted"; }

        @Override public AttachInfo attach(byte[] attachId, CatalogAttachRequest request) {
            return new AttachInfo("main", null, Map.of(), true, false);
        }

        @Override public long version(byte[] attachId) { return 1; }

        @Override public List<SchemaInfo> schemas(byte[] attachId) {
            return List.of(new SchemaInfo(null, Map.of(), List.of("main"), Map.of()));
        }

        @Override public List<TableInfo> tables(byte[] attachId, List<String> path) {
            return path.equals(List.of("main")) ? List.of(table) : List.of();
        }
    }

    private static TableInfo tableInfo(byte[] columns, List<byte[]> constraints) {
        return new TableInfo(null, Map.of(), "t", List.of("main"), columns, constraints,
                constraints.isEmpty() ? null : REVISION, Map.of(), false, null, null, null, null,
                null, null, null, null, List.of());
    }

    @Test
    void hostedCatalogNegotiatesFromItsTables() {
        TableInfo info = tableInfo(columns(Schemas.nonNull("id", Schemas.INT64)),
                List.of(fact("main", "t", "id_nn", "id").encode()));
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalog(new Fixed(info)));
        CatalogAttachResult r = svc.catalog_attach(request("hosted", client(READ, SCALAR)), null);
        assertNegotiated(r);
        assertEquals(1, svc.catalog_schema_contents_tables(r.attach_opaque_data(), List.of("main"), null, null)
                .items().size());

        TableInfo bare = tableInfo(columns(Schemas.nullable("id", Schemas.INT64)), List.of());
        VgiServiceImpl plain = service(Worker.builder().catalogName("cat").registerCatalog(new Fixed(bare)));
        assertNull(plain.catalog_attach(request("hosted", client(READ, SCALAR)), null).catalog_capabilities());
    }

    // ---- serve-time rules --------------------------------------------------------------

    private static void serve(byte[] columns, ConstraintRecord... records) {
        TableInfoSerializer.serialize(tableInfo(columns,
                java.util.Arrays.stream(records).map(ConstraintRecord::encode).toList()));
    }

    private static String refused(byte[] columns, ConstraintRecord... records) {
        return assertThrows(IllegalArgumentException.class, () -> serve(columns, records)).getMessage();
    }

    @Test
    void onlyEnforcedAllRowsRecordsAreServed() {
        byte[] cols = columns(Schemas.nullable("nick", Schemas.UTF8));
        for (var e : Enforcement.values()) {
            for (var v : Validation.values()) {
                ConstraintRecord r = ConstraintRecord.builder("main", "t", "nick_unique").unique("nick")
                        .enforcement(e).validation(v).build();
                if (r.isEngineFact()) {
                    assertDoesNotThrow(() -> serve(cols, r));
                } else {
                    String message = refused(cols, r);
                    assertTrue(message.contains("'main.t'") && message.contains("nick_unique")
                            && message.contains("ENFORCED + ALL_ROWS"), message);
                }
            }
        }
    }

    private static List<Field> served(byte[] columns, ConstraintRecord... records) {
        TableInfo info = tableInfo(columns, java.util.Arrays.stream(records).map(ConstraintRecord::encode).toList());
        return SchemaUtil.deserializeSchema(farm.query.vgi.client.TableInfoDecoder.decode(
                TableInfoSerializer.serialize(info)).columns()).getFields();
    }

    @Test
    void nullabilityIsServedToMatchTheRecords() {
        // Nothing attests an unnamed non-nullable field: it is served nullable.
        assertTrue(served(columns(Schemas.nonNull("id", Schemas.INT64))).get(0).isNullable());
        // A record attests a nullable field: it is served non-nullable.
        assertTrue(!served(columns(Schemas.nullable("id", Schemas.INT64)), fact("main", "t", "id_nn", "id"))
                .get(0).isNullable());

        ConstraintRecord pk = ConstraintRecord.builder("main", "t", "pk").primaryKey("id")
                .enforcement(Enforcement.ENFORCED).validation(Validation.ALL_ROWS).build();
        assertTrue(!served(columns(Schemas.nonNull("id", Schemas.INT64)), pk).get(0).isNullable());

        Field rowId = new Field("rowid", new FieldType(false, Schemas.INT64, null, Map.of("is_row_id", "true")), null);
        assertTrue(!served(columns(rowId, Schemas.nullable("v", Schemas.UTF8))).get(0).isNullable());

        String message = refused(columns(Schemas.nullable("v", Schemas.UTF8)), fact("main", "t", "gone_nn", "gone"));
        assertTrue(message.contains("names column gone"), message);
    }

    private static Field struct(String name, boolean nullable, Map<String, String> metadata, Field... children) {
        return new Field(name, new FieldType(nullable, new ArrowType.Struct(), null, metadata), List.of(children));
    }

    @Test
    void nullabilityRecursesIntoOrdinaryStructsOnly() {
        Field xmin = Schemas.nonNull("xmin", Schemas.FLOAT64);
        Field xmax = Schemas.nullable("xmax", Schemas.FLOAT64);
        byte[] ordinary = columns(struct("bbox", true, null, xmin, xmax));
        Field bbox = served(ordinary, fact("main", "t", "xmin_nn", "bbox", "xmin")).get(0);
        assertTrue(bbox.isNullable() && !bbox.getChildren().get(0).isNullable() && bbox.getChildren().get(1).isNullable());
        assertTrue(served(ordinary).get(0).getChildren().get(0).isNullable());

        byte[] nested = columns(struct("outer", false, null, struct("inner", true, null, Schemas.nullable("leaf", Schemas.INT32))));
        Field outer = served(nested, fact("main", "t", "outer_nn", "outer"),
                fact("main", "t", "leaf_nn", "outer", "inner", "leaf")).get(0);
        assertTrue(!outer.isNullable() && outer.getChildren().get(0).isNullable()
                && !outer.getChildren().get(0).getChildren().get(0).isNullable());

        // A logical-type struct is opaque storage: its members are neither rewritten nor addressable.
        for (String key : List.of("ARROW:extension:name", "vgi.logical_type.name")) {
            byte[] opaque = columns(struct("geom", true, Map.of(key, "x"), xmin));
            assertTrue(!served(opaque).get(0).getChildren().get(0).isNullable());
            assertTrue(refused(opaque, fact("main", "t", "x_nn", "geom", "xmin")).contains("names column geom.xmin"));
        }
    }

    @Test
    void recordsMustBelongToTheServedTable() {
        byte[] cols = columns(Schemas.nonNull("id", Schemas.INT64));
        assertTrue(refused(cols, fact("main", "other", "id_nn", "id")).contains("owned by 'main.other'"));
        assertTrue(refused(cols, fact("main", "t", "id_nn", "id"), fact("main", "t", "id_nn", "id"))
                .contains("duplicate constraint id"));
    }

    @Test
    void legacyDescriptorFieldsNameTheReplacement() {
        CatalogTable legacy = CatalogTable.builder("main", "t", columns(Schemas.nullable("id", Schemas.INT64)))
                .primaryKey(List.of(List.of(0))).build();
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalogTable(legacy));
        byte[] attach = svc.catalog_attach(request("cat", null), null).attach_opaque_data();
        var e = assertThrows(UnsupportedOperationException.class,
                () -> svc.catalog_schema_contents_tables(attach, List.of("main"), null, null));
        assertTrue(e.getMessage().contains("ConstraintRecord.builder"), e.getMessage());
    }
}
