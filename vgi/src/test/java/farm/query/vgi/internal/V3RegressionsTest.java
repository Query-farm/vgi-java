// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.CatalogInterface;
import farm.query.vgi.CatalogProfiles;
import farm.query.vgi.ReadOnlyCatalogException;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogColumns;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ColumnStatistics;
import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.catalog.ConstraintRecord.Enforcement;
import farm.query.vgi.catalog.ConstraintRecord.Kind;
import farm.query.vgi.catalog.ConstraintRecord.Validation;
import farm.query.vgi.catalog.InMemoryCatalog;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.OnConflict;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.CatalogObjectRef;
import farm.query.vgi.protocol.ConstraintDefinition;
import farm.query.vgi.protocol.ConstraintInfo;
import farm.query.vgi.protocol.ForeignKeyDefinition;
import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableCreateRequest;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.MethodNotImplementedError;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The defects the vgi.v3 audit found, each pinned. */
final class V3RegressionsTest {

    private static final String READ = CatalogProfiles.CONSTRAINT_RECORDS_READ;
    private static final String SCALAR = CatalogProfiles.SCALAR_EXPRESSION_READ;
    private static final byte[] REVISION = "rev1".getBytes(StandardCharsets.UTF_8);
    private static final List<String> MAIN = List.of("main");

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    private static byte[] columns(Field... fields) {
        return SchemaUtil.serializeSchema(Schemas.of(fields));
    }

    private static ConstraintRecord.Builder rec(String table, String id) {
        return ConstraintRecord.builder("main", table, id);
    }

    private static ConstraintRecord fact(ConstraintRecord.Builder b) {
        return b.enforcement(Enforcement.ENFORCED).validation(Validation.ALL_ROWS).build();
    }

    private static TableInfo table(String name, byte[] columns, List<byte[]> constraints) {
        return new TableInfo(null, Map.of(), name, MAIN, columns, constraints,
                constraints.isEmpty() ? null : REVISION, Map.of(), false, null, null, null, null,
                null, null, null, null, List.of());
    }

    private static TableInfo table(byte[] columns, ConstraintRecord... records) {
        return table("t", columns, java.util.Arrays.stream(records).map(ConstraintRecord::encode).toList());
    }

    private static List<Field> served(TableInfo info) {
        return SchemaUtil.deserializeSchema(TableInfoDecoder.decode(TableInfoSerializer.serialize(info)).columns())
                .getFields();
    }

    private static String refused(TableInfo info) {
        return assertThrows(IllegalArgumentException.class, () -> TableInfoSerializer.serialize(info)).getMessage();
    }

    private static Field struct(String name, boolean nullable, Map<String, String> metadata, Field... children) {
        return new Field(name, new FieldType(nullable, new ArrowType.Struct(), null, metadata), List.of(children));
    }

    // ---- time travel: records re-owned, statistics and filters carried -----------------

    @Test
    void versionedTableServesItsVariantRecordsAsItsOwn() {
        byte[] cols = columns(Schemas.nonNull("id", Schemas.INT64), Schemas.nullable("v", Schemas.UTF8));
        CatalogTable base = CatalogTable.builder("main", "versioned_constraints", cols)
                .requiredFilters(List.of(List.of(List.of("id"))))
                .constraints(List.of(fact(rec("versioned_constraints", "pk").primaryKey("id"))), REVISION).build();
        CatalogTable v1 = CatalogTable.builder("main", "versioned_constraints_v1", cols)
                .statistics(List.of(ColumnStatistics.ofInt64("id", 1, 9, false, null)))
                .constraints(List.of(fact(rec("versioned_constraints_v1", "pk").primaryKey("id")),
                        fact(rec("versioned_constraints_v1", "self_fk").foreignKey(List.of("id"),
                                "versioned_constraints_v1", List.of("id"), "pk"))), REVISION).build();
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalogTable(base)
                .registerCatalogTable(v1));
        byte[] attach = svc.catalog_attach(new CatalogAttachRequest("cat", null, null, null,
                CatalogProfiles.defaultClientOffer()), null).attach_opaque_data();
        TableInfo info = TableInfoDecoder.decode(svc.catalog_table_get(attach, MAIN, "versioned_constraints",
                "version", "1", null, null).items().get(0));
        List<ConstraintRecord> records = ConstraintRecord.decodeAll(info.constraints());
        assertEquals(2, records.size());
        for (ConstraintRecord r : records) assertEquals("versioned_constraints", r.table());
        assertEquals("versioned_constraints", records.get(1).references().table());
        assertNotNull(info.column_statistics());
        assertTrue(info.supports_column_statistics());
        assertEquals(List.of(List.of(List.of("id"))), info.required_filters());
    }

    // ---- nullability: relax, never claim -----------------------------------------------

    @Test
    void unnamedNonNullableFieldsAreServedNullable() {
        Field rowId = new Field("rowid", new FieldType(false, Schemas.INT64, null, Map.of("is_row_id", "true")), null);
        Field opaque = struct("geom", false, Map.of("ARROW:extension:name", "x"), Schemas.nonNull("inner", Schemas.INT32));
        List<Field> fields = served(table(columns(rowId, Schemas.nonNull("id", Schemas.INT64),
                struct("bbox", false, null, Schemas.nonNull("xmin", Schemas.FLOAT64)), opaque)));
        assertFalse(fields.get(0).isNullable(), "the row id is untouched");
        assertTrue(fields.get(1).isNullable());
        assertTrue(fields.get(2).isNullable());
        assertTrue(fields.get(2).getChildren().get(0).isNullable(), "nested child relaxed too");
        assertTrue(fields.get(3).isNullable());
        assertFalse(fields.get(3).getChildren().get(0).isNullable(), "opaque storage members are left alone");
    }

    @Test
    void recordNamedFieldsAreServedNonNullable() {
        List<Field> fields = served(table(columns(Schemas.nullable("id", Schemas.INT64),
                        struct("bbox", true, null, Schemas.nullable("xmin", Schemas.FLOAT64))),
                fact(rec("t", "pk").primaryKey("id")), fact(rec("t", "x_nn").notNull("bbox", "xmin"))));
        assertFalse(fields.get(0).isNullable());
        assertTrue(fields.get(1).isNullable(), "a nested fact never makes its parent required");
        assertFalse(fields.get(1).getChildren().get(0).isNullable());
    }

    @Test
    void functionBackedTableOutputSchemaIsRelaxed() {
        CatalogTable fn = CatalogTable.functionBacked("main", "t",
                columns(Schemas.nonNull("n", Schemas.INT64)), "", "sequence");
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalogTable(fn));
        byte[] attach = svc.catalog_attach(new CatalogAttachRequest("cat", null, null, null, null), null)
                .attach_opaque_data();
        TableInfo info = TableInfoDecoder.decode(
                svc.catalog_schema_contents_tables(attach, MAIN, null, null).items().get(0));
        assertTrue(SchemaUtil.deserializeSchema(info.columns()).getFields().get(0).isNullable());
    }

    @Test
    void providerCatalogContentsTablesAreRelaxedToo() {
        byte[] item = RecordCodec.serializeToBytes(table(columns(Schemas.nonNull("id", Schemas.INT64))));
        byte[] stream = farm.query.vgi.MetadataCollections.merge(List.of(
                farm.query.vgi.MetadataCollections.encode(List.of(RecordCodec.deserializeFromBytes(item,
                        TableInfo.class)), TableInfo.class)), TableInfo.class);
        TableInfo info = farm.query.vgi.MetadataCollections.decode(stream, TableInfo.class).get(0);
        assertTrue(SchemaUtil.deserializeSchema(info.columns()).getFields().get(0).isNullable());
    }

    // ---- ATTACH rules ------------------------------------------------------------------

    private static ConstraintRecord raw(String id, Kind kind, List<List<String>> columns, String declaredName) {
        return new ConstraintRecord(MAIN, "t", id, kind, columns, Enforcement.ENFORCED, Validation.ALL_ROWS,
                declaredName, null, null);
    }

    @Test
    void attachRulesAreCheckedBeforeServing() {
        byte[] cols = columns(Schemas.nullable("a", Schemas.INT64), Schemas.nullable("b", Schemas.INT64),
                struct("s", true, null, Schemas.nullable("x", Schemas.INT64)));
        String m = refused(table(cols, raw("u1", Kind.UNIQUE, List.of(List.of("a")), "Key"),
                raw("u2", Kind.UNIQUE, List.of(List.of("b")), "key")));
        assertTrue(m.contains("'main.t'") && m.contains("u2") && m.contains("duplicate declared name"), m);
        m = refused(table(cols, fact(rec("t", "pk1").primaryKey("a")), fact(rec("t", "pk2").primaryKey("b"))));
        assertTrue(m.contains("pk2") && m.contains("PRIMARY KEY"), m);
        m = refused(table(cols, raw("u", Kind.UNIQUE, List.of(List.of("s", "x")), null)));
        assertTrue(m.contains("nested column s.x"), m);
        m = refused(table(cols, raw("u", Kind.UNIQUE, List.of(List.of("gone")), null)));
        assertTrue(m.contains("column gone"), m);
        m = refused(table(cols, fact(rec("t", "nn").notNull("s", "gone"))));
        assertTrue(m.contains("s.gone"), m);
        ConstraintRecord twoPart = new ConstraintRecord(List.of("a", "b"), "t", "nn", Kind.NOT_NULL,
                List.of(List.of("a")), Enforcement.ENFORCED, Validation.ALL_ROWS, null, null, null);
        TableInfo deep = new TableInfo(null, Map.of(), "t", List.of("a", "b"), cols, List.of(twoPart.encode()),
                REVISION, Map.of(), false, null, null, null, null, null, null, null, null, List.of());
        assertTrue(refused(deep).contains("one-component"));
    }

    @Test
    void builderRefusesWhatDuckDbCannotReadEarly() {
        assertThrows(IllegalArgumentException.class, () -> fact(ConstraintRecord.builder(List.of("a", "b"), "t", "x")
                .notNull("c")));
        assertThrows(IllegalArgumentException.class, () -> fact(rec("t", "u").of(Kind.UNIQUE,
                List.of(List.of("s", "x")))));
        assertThrows(IllegalArgumentException.class, () -> fact(rec("t", "fk").foreignKey(List.of(List.of("a")),
                new ConstraintRecord.ForeignKeyTarget(List.of("other"), "p", List.of(List.of("id")), "pk"))));
    }

    private static byte[] foreignKey(String action, List<String> referencedSchema) {
        byte[] table = RecordCodec.serializeToBytes(new CatalogObjectRef("table", referencedSchema, "p", null));
        byte[] fk = RecordCodec.serializeToBytes(new ForeignKeyDefinition(table, List.of(List.of("id")), "SIMPLE",
                action, "NO_ACTION"));
        byte[] def = RecordCodec.serializeToBytes(new ConstraintDefinition("FOREIGN_KEY", List.of(List.of("a")),
                null, null, fk));
        return RecordCodec.serializeToBytes(new ConstraintInfo(
                RecordCodec.serializeToBytes(new CatalogObjectRef("constraint", MAIN, "fk", "t")), def, "ENFORCED",
                "ALL_ROWS", RecordCodec.serializeToBytes(new CatalogObjectRef("constraint", referencedSchema, "pk", "p"))));
    }

    @Test
    void foreignKeysAreCheckedAcrossTheCatalog() {
        byte[] cols = columns(Schemas.nullable("a", Schemas.INT64));
        assertTrue(refused(table("t", cols, List.of(foreignKey("CASCADE", MAIN)))).contains("CASCADE"));
        assertTrue(refused(table("t", cols, List.of(foreignKey("NO_ACTION", List.of("other")))))
                .contains("same schema"));
        assertDoesNotThrow(() -> TableInfoSerializer.serialize(table("t", cols, List.of(foreignKey("RESTRICT", MAIN)))));

        // Through the service, the referenced key must exist.
        byte[] pcols = columns(Schemas.nonNull("id", Schemas.INT64));
        CatalogTable child = CatalogTable.builder("main", "t", cols).constraints(List.of(
                fact(rec("t", "fk").foreignKey(List.of("a"), "p", List.of("id"), "pk"))), REVISION).build();
        for (String keyId : List.of("pk", "other")) {
            CatalogTable parent = CatalogTable.builder("main", "p", pcols)
                    .constraints(List.of(fact(rec("p", keyId).primaryKey("id"))), REVISION).build();
            VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalogTable(parent)
                    .registerCatalogTable(child));
            byte[] attach = svc.catalog_attach(new CatalogAttachRequest("cat", null, null, null, null), null)
                    .attach_opaque_data();
            if (keyId.equals("pk")) {
                assertEquals(2, svc.catalog_schema_contents_tables(attach, MAIN, null, null).items().size());
            } else {
                var e = assertThrows(IllegalArgumentException.class,
                        () -> svc.catalog_schema_contents_tables(attach, MAIN, null, null));
                assertTrue(e.getMessage().contains("'pk'") && e.getMessage().contains("does not declare"), e.getMessage());
            }
        }
    }

    @Test
    void checkExpressionsFollowTheNumericComparisonGrammar() {
        byte[] cols = columns(Schemas.nullable("budget", Schemas.INT64), Schemas.nullable("rate", Schemas.FLOAT64),
                Schemas.nullable("name", Schemas.UTF8), Schemas.nullable("u", Schemas.UINT32));
        for (String ok : List.of("budget >= 0", "(budget > -5)", "BUDGET <> rate", "\"budget\" == 1.5e3",
                "(budget) < (100)", "rate != .5")) {
            assertDoesNotThrow(() -> TableInfoSerializer.serialize(table(cols, fact(rec("t", "c").check(ok)))), ok);
        }
        for (String bad : List.of("budget >= 0 AND budget < 9", "name = 'x'", "u > 0", "abs(budget) > 0",
                "budget::INT > 0", "t.budget > 0", "budget > 0 > 1", "gone > 0", "budget BETWEEN 0 AND 1",
                "budget + 1 > 0", "budget IS NULL", "budget > true")) {
            String m = refused(table(cols, fact(rec("t", "c").check(bad))));
            assertTrue(m.contains("'c'") && m.contains("CHECK"), bad + " -> " + m);
        }
    }

    // ---- generated columns -------------------------------------------------------------

    @Test
    void legacyGeneratedExpressionIsTranslated() {
        Field legacy = new Field("doubled", new FieldType(false, Schemas.INT64, null,
                Map.of("generated_expression", "n * 2")), null);
        List<Field> fields = served(table(columns(Schemas.nullable("n", Schemas.INT64), legacy)));
        Map<String, String> md = fields.get(1).getMetadata();
        assertFalse(md.containsKey("generated_expression"));
        assertEquals("virtual", md.get(ColumnExpressions.GENERATION));
        assertNotNull(md.get(ColumnExpressions.GENERATED));
        assertTrue(fields.get(1).isNullable());
    }

    @Test
    void generatedColumnsMirrorTheExtensionRestrictions() {
        Field ok = CatalogColumns.virtualGenerated("label", Schemas.UTF8, "'item_' || n::VARCHAR", "main");
        assertTrue(ok.isNullable());
        assertDoesNotThrow(() -> CatalogColumns.virtualGenerated("b", Schemas.BOOL,
                "coalesce(n, 0) > 1 AND NOT (n IS NULL) OR lower(s) = 'x'", "main"));
        assertDoesNotThrow(() -> CatalogColumns.virtualGenerated("i", Schemas.INT32, "CAST(n AS INTEGER) % 3", "main"));
        for (String bad : List.of("n / 2", "n * 1.5", "TRY_CAST(n AS INT)", "CASE WHEN n THEN 1 END",
                "length(s)", "n::DOUBLE", "n IN (1, 2)", "x.n + 1")) {
            assertThrows(IllegalArgumentException.class,
                    () -> CatalogColumns.virtualGenerated("g", Schemas.INT64, bad, "main"), bad);
        }
        assertThrows(IllegalArgumentException.class,
                () -> CatalogColumns.virtualGenerated("g", Schemas.FLOAT64, "n", "main"));
        assertThrows(IllegalArgumentException.class,
                () -> CatalogColumns.virtualGenerated("g", Schemas.UINT32, "n", "main"));
        // A NOT_NULL record cannot make a generated column non-nullable.
        String m = refused(table(columns(Schemas.nullable("n", Schemas.INT64),
                CatalogColumns.virtualGenerated("g", Schemas.INT64, "n * 2", "main")),
                fact(rec("t", "g_nn").notNull("g"))));
        assertTrue(m.contains("nullable"), m);
    }

    // ---- CatalogInterface.viewCreate: an override at any level is reached ---------------

    private abstract static class Base implements CatalogInterface {
        final List<String> calls = new ArrayList<>();
        @Override public String name() { return "c"; }
        @Override public AttachInfo attach(byte[] a, CatalogAttachRequest r) { return new AttachInfo("main", null, Map.of(), false, false); }
        @Override public long version(byte[] a) { return 1; }
        @Override public List<SchemaInfo> schemas(byte[] a) { return List.of(); }
    }

    private static void create(CatalogInterface c, List<String> aliases, String comment) {
        c.viewCreate(new byte[16], MAIN, "v", "SELECT 1", OnConflict.ERROR, aliases, comment, null, null);
    }

    @Test
    @SuppressWarnings("deprecation")
    void viewCreateReachesEveryOverrideLevel() {
        Base five = new Base() {
            @Override public void viewCreate(byte[] a, List<String> p, String n, String d, OnConflict o) { calls.add("5"); }
        };
        create(five, List.of(), null);
        assertEquals(List.of("5"), five.calls);
        assertThrows(UnsupportedOperationException.class, () -> create(five, List.of("x"), null));
        assertThrows(UnsupportedOperationException.class, () -> create(five, List.of(), "comment"));

        Base six = new Base() {
            @Override public void viewCreate(byte[] a, List<String> p, String n, String d, OnConflict o, List<String> al) {
                calls.add("6:" + al);
            }
        };
        create(six, List.of("x"), null);
        assertEquals(List.of("6:[x]"), six.calls);

        Base nine = new Base() {
            @Override public void viewCreate(byte[] a, List<String> p, String n, String d, OnConflict o, List<String> al,
                                             String c, Map<String, String> t, Map<String, String> cc) {
                calls.add("9:" + c);
            }
        };
        create(nine, List.of(), "hi");
        assertEquals(List.of("9:hi"), nine.calls);

        Base none = new Base() {};
        assertThrows(ReadOnlyCatalogException.class, () -> create(none, List.of(), null));
        assertThrows(ReadOnlyCatalogException.class, () -> create(none, List.of("x"), "c"));

        // Through the service, the 6-argument override is reached too.
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalog(six));
        byte[] attach = svc.catalog_attach(new CatalogAttachRequest("c", null, null, null, null), null)
                .attach_opaque_data();
        svc.catalog_view_create(attach, MAIN, "v", "SELECT 1", "ERROR", null, List.of("y"), "regular", null, null,
                null, null);
        assertEquals(List.of("6:[x]", "6:[y]"), six.calls);
    }

    // ---- hosted CREATE TABLE gated on the catalog's own negotiation ---------------------

    private static final class Enforcing extends Base {
        final List<TableCreateRequest> created = new ArrayList<>();
        @Override public Worker.CatalogCapabilitySelection catalogCapabilities(byte[] a, CatalogAttachRequest r) {
            return CatalogProfiles.negotiate(new CatalogCapabilities(List.of(READ), List.of(CatalogProfiles.CONSTRAINT_CREATE),
                    List.of(CatalogProfiles.CONSTRAINT_ENFORCE), List.of()), r.client_capabilities());
        }
        @Override public void tableCreate(byte[] a, TableCreateRequest request) { created.add(request); }
    }

    private static byte[] constrainedCreate(byte[] attach) {
        byte[] def = RecordCodec.serializeToBytes(new ConstraintDefinition("NOT_NULL", List.of(List.of("id")),
                null, null, null));
        return RecordCodec.serializeToBytes(new TableCreateRequest(attach, MAIN, "t",
                columns(Schemas.nonNull("id", Schemas.INT64), new Field("p",
                        new FieldType(true, Schemas.BINARY, null, Map.of("ARROW:extension:name", "geoarrow.wkb")), null)),
                "ERROR", List.of(def), null));
    }

    @Test
    void hostedCreateTableReachesACatalogThatNegotiatedConstraintDdl() {
        Enforcing catalog = new Enforcing();
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalog(catalog));
        byte[] offer = CatalogProfiles.clientOffer("duckdb", new CatalogCapabilities(List.of(READ, SCALAR),
                List.of(CatalogProfiles.CONSTRAINT_CREATE), List.of(CatalogProfiles.CONSTRAINT_ENFORCE), List.of()));
        CatalogAttachResult r = svc.catalog_attach(new CatalogAttachRequest("c", null, null, null, offer), null);
        CatalogCapabilities selected = CatalogProfiles.decode(r.selected_capabilities());
        assertEquals(List.of(CatalogProfiles.CONSTRAINT_CREATE), selected.ddl());
        svc.catalog_table_create(constrainedCreate(r.attach_opaque_data()), null);
        assertEquals(1, catalog.created.size());

        // The client offered only reads: the gate refuses before the catalog sees it.
        byte[] reader = svc.catalog_attach(new CatalogAttachRequest("c", null, null, null,
                CatalogProfiles.defaultClientOffer()), null).attach_opaque_data();
        var e = assertThrows(MethodNotImplementedError.class,
                () -> svc.catalog_table_create(constrainedCreate(reader), null));
        assertTrue(e.getMessage().contains(CatalogProfiles.CONSTRAINT_CREATE), e.getMessage());
        assertEquals(1, catalog.created.size());
    }

    @Test
    void inMemoryCatalogStillRefusesWhatItCannotEnforce() {
        VgiServiceImpl svc = service(Worker.builder().catalogName("cat").registerCatalog(new InMemoryCatalog("mem")));
        byte[] attach = svc.catalog_attach(new CatalogAttachRequest("mem", null, null, null, null), null)
                .attach_opaque_data();
        byte[] notNull = RecordCodec.serializeToBytes(new TableCreateRequest(attach, MAIN, "t",
                columns(Schemas.nonNull("id", Schemas.INT64)), "ERROR", List.of(), null));
        var e = assertThrows(MethodNotImplementedError.class, () -> svc.catalog_table_create(notNull, null));
        assertTrue(e.getMessage().contains("NOT NULL"), e.getMessage());
    }

    // ---- capabilities ------------------------------------------------------------------

    @Test
    void profileSelectionHonoursPrerequisites() {
        CatalogCapabilities all = new CatalogCapabilities(List.of(READ, SCALAR), List.of(CatalogProfiles.CONSTRAINT_CREATE),
                List.of(CatalogProfiles.CONSTRAINT_ENFORCE), List.of());
        byte[] noRead = CatalogProfiles.clientOffer("x", new CatalogCapabilities(List.of(SCALAR),
                List.of(CatalogProfiles.CONSTRAINT_CREATE), List.of(CatalogProfiles.CONSTRAINT_ENFORCE), List.of()));
        assertEquals(CatalogProfiles.none(), CatalogProfiles.select(all, noRead));
        assertEquals(List.of(READ, SCALAR), CatalogProfiles.select(all, CatalogProfiles.defaultClientOffer()).read());
        assertEquals(CatalogProfiles.none(), CatalogProfiles.select(all, null));
    }

    @Test
    void extraCatalogCapabilitySelectorIsMerged() {
        Worker w = Worker.builder().catalogName("cat")
                .registerExtraCatalog(new Worker.ExtraCatalog("aux", null, null, "aux").withCapabilities(r ->
                        CatalogProfiles.negotiate(new CatalogCapabilities(List.of(), List.of(), List.of(),
                                List.of(CatalogProfiles.TRANSACTION_DDL)), r.client_capabilities())))
                .registerExtraCatalogTable("aux", CatalogTable.builder("main", "t",
                        columns(Schemas.nullable("id", Schemas.INT64)))
                        .constraints(List.of(fact(rec("t", "pk").primaryKey("id"))), REVISION).build());
        CatalogAttachResult r = service(w).catalog_attach(new CatalogAttachRequest("aux", null, null, null,
                CatalogProfiles.clientOffer("x", new CatalogCapabilities(List.of(READ), List.of(), List.of(),
                        List.of(CatalogProfiles.TRANSACTION_DDL)))), null);
        CatalogCapabilities selected = CatalogProfiles.decode(r.selected_capabilities());
        assertEquals(List.of(READ), selected.read());
        assertEquals(List.of(CatalogProfiles.TRANSACTION_DDL), selected.transaction());
        assertTrue(CatalogProfiles.decode(r.catalog_capabilities()).read().contains(SCALAR));
    }

    // ---- macros ------------------------------------------------------------------------

    @Test
    void macroInfoOfAndLiteralOnlyDefaults() {
        Macro ok = new Macro("main", "add_n", MacroType.SCALAR, List.of("x", "n"), Map.of("n", "1"), Map.of(),
                "x + n", "", Map.of());
        MacroInfo info = MacroInfo.of(ok);
        assertEquals("add_n", info.name());
        assertEquals(1, info.overloads().size());
        Macro expression = new Macro("main", "days", MacroType.SCALAR, List.of("d"), Map.of("d", "current_date"),
                Map.of(), "d", "", Map.of());
        var e = assertThrows(IllegalArgumentException.class, () -> Worker.builder().registerMacro(expression));
        assertTrue(e.getMessage().contains("current_date"), e.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> Worker.builder().registerExtraCatalogMacro("aux", expression));
    }
}
