// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import farm.query.vgi.catalog.ConstraintRecord.Enforcement;
import farm.query.vgi.catalog.ConstraintRecord.Kind;
import farm.query.vgi.catalog.ConstraintRecord.Validation;
import farm.query.vgi.protocol.CatalogObjectRef;
import farm.query.vgi.protocol.ConstraintDefinition;
import farm.query.vgi.protocol.ConstraintInfo;
import farm.query.vgi.protocol.ForeignKeyDefinition;
import farm.query.vgi.protocol.SqlExpression;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ConstraintRecordTest {

    private static ConstraintRecord.Builder fact(String id) {
        return ConstraintRecord.builder("data", "employees", id)
                .enforcement(Enforcement.ENFORCED).validation(Validation.ALL_ROWS);
    }

    @Test
    void everyKindRoundTripsThroughTheWireRecord() {
        List<ConstraintRecord> records = List.of(
                fact("id_nn").notNull("id").build(),
                fact("xmin_nn").notNull("bbox", "xmin").build(),
                fact("pk").primaryKey("id").declaredName("employees_pk").build(),
                fact("email_unique").unique("email", "region").build(),
                fact("budget_check").check("budget >= 0").build(),
                fact("dept_fk").foreignKey(List.of("department_id"), "departments", List.of("id"), "primary").build(),
                ConstraintRecord.builder(List.of("data"), "employees", "soft").unique("nick")
                        .enforcement(Enforcement.METADATA_ONLY).validation(Validation.UNVALIDATED).build());
        for (ConstraintRecord r : records) assertEquals(r, ConstraintRecord.decode(r.encode()));
    }

    @Test
    void nestedRecordsCarryTheCanonicalWireShape() {
        byte[] bytes = fact("dept_fk")
                .foreignKey(List.of("department_id"), "departments", List.of("id"), "primary").build().encode();
        ConstraintInfo info = RecordCodec.deserializeFromBytes(bytes, ConstraintInfo.class);
        assertEquals("ENFORCED", info.enforcement());
        assertEquals("ALL_ROWS", info.validation());
        assertEquals(new CatalogObjectRef("constraint", List.of("data"), "dept_fk", "employees"),
                RecordCodec.deserializeFromBytes(info.ref(), CatalogObjectRef.class));
        assertEquals(new CatalogObjectRef("constraint", List.of("data"), "primary", "departments"),
                RecordCodec.deserializeFromBytes(info.referenced_constraint(), CatalogObjectRef.class));
        ConstraintDefinition def = RecordCodec.deserializeFromBytes(info.definition(), ConstraintDefinition.class);
        assertEquals("FOREIGN_KEY", def.kind());
        assertEquals(List.of(List.of("department_id")), def.column_paths());
        ForeignKeyDefinition fk = RecordCodec.deserializeFromBytes(def.foreign_key(), ForeignKeyDefinition.class);
        assertEquals("SIMPLE", fk.match());
        assertEquals("NO_ACTION", fk.on_update());
        assertEquals(new CatalogObjectRef("table", List.of("data"), "departments", null),
                RecordCodec.deserializeFromBytes(fk.referenced_table(), CatalogObjectRef.class));

        ConstraintDefinition check = RecordCodec.deserializeFromBytes(RecordCodec.deserializeFromBytes(
                fact("c").check("budget >= 0").build().encode(), ConstraintInfo.class).definition(),
                ConstraintDefinition.class);
        SqlExpression sql = RecordCodec.deserializeFromBytes(check.check_expression(), SqlExpression.class);
        assertEquals(ConstraintRecord.SCALAR_EXPRESSION_READ, sql.profile());
        assertEquals(List.of("data"), sql.schema_path());
        assertEquals("UTC", sql.evaluation_context().timezone());
        assertTrue(check.column_paths().isEmpty());
        assertNull(check.foreign_key());
    }

    @Test
    void attestationMustBeExplicit() {
        assertThrows(IllegalStateException.class,
                () -> ConstraintRecord.builder("data", "t", "a").notNull("id").validation(Validation.ALL_ROWS).build());
        assertThrows(IllegalStateException.class,
                () -> ConstraintRecord.builder("data", "t", "a").notNull("id").enforcement(Enforcement.ENFORCED).build());
        assertThrows(IllegalStateException.class, () -> fact("a").build());
        assertTrue(fact("a").notNull("id").build().isEngineFact());
    }

    @Test
    void malformedRecordsAreRejected() {
        assertThrows(IllegalStateException.class, () -> fact("a").notNull("id").unique("x"));
        assertThrows(IllegalArgumentException.class, () -> fact("").notNull("id").build());
        assertThrows(IllegalArgumentException.class, () -> fact("a\0b").notNull("id").build());
        assertThrows(IllegalArgumentException.class, () -> fact("a").primaryKey().build());
        assertThrows(IllegalArgumentException.class, () -> fact("a").unique("x", "x").build());
        assertThrows(IllegalArgumentException.class, () -> fact("a").of(Kind.NOT_NULL,
                List.of(List.of("x"), List.of("y"))).build());
        assertThrows(IllegalArgumentException.class, () -> fact("a").of(Kind.CHECK, List.of()));
        assertThrows(IllegalArgumentException.class, () -> fact("a").check("").build());
        assertThrows(IllegalArgumentException.class, () -> fact("a")
                .foreignKey(List.of("x", "y"), "p", List.of("id"), "pk").build());
        assertThrows(IllegalArgumentException.class, () -> ConstraintRecord.decode(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> ConstraintRecord.decode(new byte[] {1, 2, 3}));
    }

    @Test
    void tablesTakeTypedRecords() {
        ConstraintRecord pk = fact("pk").primaryKey("id").build();
        byte[] revision = "r1".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CatalogTable built = CatalogTable.builder("data", "employees", new byte[0])
                .constraints(List.of(pk), revision).build();
        assertEquals(pk, ConstraintRecord.decode(built.constraints().get(0)));
        CatalogTable copied = CatalogTable.builder("data", "employees", new byte[0]).build()
                .withConstraintRecords(List.of(pk), revision);
        assertEquals(pk, ConstraintRecord.decode(copied.constraints().get(0)));
    }
}
