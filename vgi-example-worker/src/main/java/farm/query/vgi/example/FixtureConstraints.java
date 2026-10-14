// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.example;

import farm.query.vgi.protocol.*;
import farm.query.vgirpc.marshal.RecordCodec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** Explicit fixture identities; declarations do not claim enforcement or row validation. */
final class FixtureConstraints {
    static final byte[] REVISION = "fixture.constraints.v1".getBytes(StandardCharsets.UTF_8);

    private FixtureConstraints() {}

    private static byte[] ref(String kind, String name, String owner) {
        return RecordCodec.serializeToBytes(new CatalogObjectRef(kind, List.of("data"), name, owner));
    }

    static byte[] of(String table, String key, String kind, String... columns) {
        return record(table, key, new ConstraintDefinition(kind,
                Arrays.stream(columns).map(List::of).toList(), null, null, null), null);
    }

    static byte[] check(String table, String key, String sql) {
        byte[] expression = RecordCodec.serializeToBytes(new SqlExpression(sql,
                "vgi.expression.duckdb.scalar.v1", List.of("data"),
                new QueryEvaluationContext("UTC", "binary", "asc", "nulls_last", true)));
        return record(table, key, new ConstraintDefinition("CHECK", List.of(), null, expression, null), null);
    }

    static byte[] departmentForeignKey(String table) {
        byte[] foreign = RecordCodec.serializeToBytes(new ForeignKeyDefinition(
                ref("table", "departments", null), List.of(List.of("id")),
                "SIMPLE", "NO_ACTION", "NO_ACTION"));
        return record(table, "department_fk", new ConstraintDefinition("FOREIGN_KEY",
                List.of(List.of("department_id")), null, null, foreign),
                ref("constraint", "primary", "departments"));
    }

    private static byte[] record(String table, String key, ConstraintDefinition definition, byte[] target) {
        return RecordCodec.serializeToBytes(new ConstraintInfo(ref("constraint", key, table),
                RecordCodec.serializeToBytes(definition), "METADATA_ONLY", "UNVALIDATED", target));
    }
}
