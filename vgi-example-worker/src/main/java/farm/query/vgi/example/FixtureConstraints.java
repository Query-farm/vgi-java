// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.example;

import farm.query.vgi.protocol.*;
import farm.query.vgirpc.marshal.RecordCodec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import farm.query.vgi.example.table.CannedDataFunction;

/** Facts checked against the complete immutable dataset emitted by the read-only fixture. */
final class FixtureConstraints {
    static final byte[] REVISION = "fixture.constraints.v1".getBytes(StandardCharsets.UTF_8);

    private FixtureConstraints() {}

    private static byte[] ref(String kind, String name, String owner) {
        return RecordCodec.serializeToBytes(new CatalogObjectRef(kind, List.of("data"), name, owner));
    }

    static List<byte[]> snapshot(String table, org.apache.arrow.vector.types.pojo.Field[] fields) {
        if (table.startsWith("versioned_constraints_v")) {
            int version = Integer.parseInt(table.substring(table.length() - 1));
            var result = new java.util.ArrayList<byte[]>();
            result.add(of(table, "id_nn", "NOT_NULL", "id"));
            if (version >= 2) {
                result.add(of(table, "name_nn", "NOT_NULL", "name"));
                result.add(of(table, "primary", "PRIMARY_KEY", "id"));
                result.add(of(table, "email_unique", "UNIQUE", "email"));
            }
            if (version >= 3) result.add(departmentForeignKey(table));
            return List.copyOf(result);
        }
        // Explicit fixture field declarations, verified against actual fixed rows.
        return Arrays.stream(fields).filter(field -> !field.isNullable() && !"true".equals(field.getMetadata().get("is_row_id")))
                .map(field -> of(table, "not-null-" + field.getName(), "NOT_NULL", field.getName())).toList();
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

    static void validate(List<Map<String, Object>> rows, ConstraintDefinition definition,
                         List<Map<String, Object>> departments) {
        var seen = new HashSet<List<Object>>();
        for (var row : rows) {
            List<Object> values = new java.util.ArrayList<>();
            for (var path : definition.column_paths()) {
                if (path.size() != 1 || !row.containsKey(path.getFirst()))
                    throw new IllegalArgumentException("Unsupported fixture constraint path");
                values.add(row.get(path.getFirst()));
            }
            boolean hasNull = values.contains(null);
            switch (definition.kind()) {
                case "NOT_NULL", "PRIMARY_KEY" -> {
                    if (hasNull) throw new IllegalArgumentException("Fixed fixture violates NOT_NULL");
                }
                case "UNIQUE", "CHECK", "FOREIGN_KEY" -> { }
                default -> throw new IllegalArgumentException("Unsupported fixture constraint");
            }
            if ((definition.kind().equals("PRIMARY_KEY") || definition.kind().equals("UNIQUE"))
                    && !hasNull && !seen.add(values))
                throw new IllegalArgumentException("Fixed fixture violates uniqueness");
            if (definition.kind().equals("CHECK")) {
                var expression = RecordCodec.deserializeFromBytes(definition.check_expression(), SqlExpression.class);
                if (!expression.sql().equals("budget >= 0"))
                    throw new IllegalArgumentException("Unsupported fixed fixture CHECK");
                Object budget = row.get("budget");
                if (budget != null && (!(budget instanceof Number n) || !(n.doubleValue() >= 0)))
                    throw new IllegalArgumentException("Fixed fixture violates budget CHECK");
            }
            if (definition.kind().equals("FOREIGN_KEY") && !hasNull
                    && departments.stream().noneMatch(parent -> java.util.Objects.equals(parent.get("id"), values.getFirst())))
                throw new IllegalArgumentException("Fixed fixture violates department reference");
        }
    }

    private static byte[] record(String table, String key, ConstraintDefinition definition, byte[] target) {
        validate(CannedDataFunction.rowsFor(table), definition, CannedDataFunction.rowsFor("departments"));
        return RecordCodec.serializeToBytes(new ConstraintInfo(ref("constraint", key, table.replaceFirst("_v(?:[123]|_[0-9]+_[0-9]+_[0-9]+)$", "")),
                RecordCodec.serializeToBytes(definition), "ENFORCED", "ALL_ROWS", target));
    }
}
