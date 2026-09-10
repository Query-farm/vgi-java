// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import farm.query.vgi.client.EncodedPushdownFilters;
import farm.query.vgi.client.FilterPredicate;
import farm.query.vgi.client.ProjectedColumn;
import farm.query.vgi.client.PushdownFiltersEncoder;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class PushdownFiltersV2Test {
    private static final Field VALUE = new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null);
    private static final Field MIDDLE = new Field("middle", FieldType.nullable(new ArrowType.Struct()),
            List.of(VALUE));
    private static final Field OUTER = new Field("outer", FieldType.nullable(new ArrowType.Struct()),
            List.of(MIDDLE));
    private static final Schema NESTED_SCHEMA = new Schema(List.of(OUTER));
    private static final Schema INT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    private static final Schema BOOL_SCHEMA = new Schema(List.of(
            new Field("flag", FieldType.nullable(new ArrowType.Bool()), null)));

    @Test
    void evaluatesArbitrarilyNestedFieldReferencesAndParentNulls() {
        EncodedPushdownFilters encoded = PushdownFiltersEncoder.builder()
                .filter(ProjectedColumn.of("outer", 0),
                        FilterPredicate.structField(0, "middle",
                                FilterPredicate.structField(0, "value", FilterPredicate.eq(7L))))
                .encode();
        PushdownFilters filters = PushdownFiltersDecoder.decode(
                encoded.pushdownFilters(), NESTED_SCHEMA, encoded.joinKeys(),
                PushdownFiltersDecoder.Capabilities.core());

        try (VectorSchemaRoot root = VectorSchemaRoot.create(NESTED_SCHEMA, Allocators.root())) {
            root.allocateNew();
            VectorScalarCodec.write(root.getVector(0), 0,
                    Map.of("middle", Map.of("value", 7L)));
            VectorScalarCodec.write(root.getVector(0), 1,
                    Map.of("middle", Map.of("value", 8L)));
            root.getVector(0).setNull(2);
            root.setRowCount(3);
            assertArrayEquals(new boolean[] {true, false, false}, filters.evaluate(root));
        }
    }

    @Test
    void rejectsMalformedUnknownNodesRegardlessOfMode() {
        String expression = "{\"node\":\"future_node\"}";
        assertThrows(FilterV2Exception.class, () -> decode(batch(snapshot(
                predicate("hint", "advisory", expression))), new Schema(List.of())));
        assertThrows(FilterV2Exception.class, () -> decode(batch(snapshot(
                predicate("query", "required", expression))), new Schema(List.of())));
    }

    @Test
    void deltaIsAtomicAndRetainsRevisionTombstones() {
        PushdownFilters state = decode(batch(snapshot(
                predicate("dynamic", "advisory", isNull("n")))), INT_SCHEMA);
        PushdownFilters removed = state.applyDelta(batch(delta(
                "{\"operation\":\"remove\",\"id\":\"dynamic\",\"revision\":1}")));
        assertEquals(0, removed.predicates().size());
        assertEquals(1L, removed.revisions().get("dynamic"));

        PushdownFilters stale = removed.applyDelta(batch(delta(
                "{\"operation\":\"remove\",\"id\":\"dynamic\",\"revision\":1}")));
        assertEquals(removed.revisions(), stale.revisions());

        String malformed = delta(
                "{\"operation\":\"remove\",\"id\":\"dynamic\",\"revision\":2}",
                "{\"operation\":\"remove\",\"id\":\"other\",\"revision\":true}");
        assertThrows(FilterV2Exception.class, () -> removed.applyDelta(batch(malformed)));
        assertEquals(1L, removed.revisions().get("dynamic"));
    }

    @Test
    void noneContextRejectsContextDependentArithmetic() {
        String divide = "{\"node\":\"comparison\",\"op\":\"eq\",\"left\":"
                + "{\"node\":\"arithmetic\",\"op\":\"divide\",\"left\":"
                + column("n") + ",\"right\":" + column("n") + "},\"right\":" + column("n") + "}";
        assertThrows(FilterV2Exception.class, () -> decode(batch(snapshot(
                predicate("query", "required", divide))), INT_SCHEMA));
    }

    @Test
    void externalInUsesArbitraryBatchAndColumnIndexes() {
        Schema outputSchema = INT_SCHEMA;
        byte[] firstBatch = externalBatch(
                new Schema(List.of(new Field("unused", FieldType.nullable(new ArrowType.Int(64, true)), null))),
                List.of(List.of(99L)));
        byte[] secondBatch = externalBatch(
                new Schema(List.of(
                        new Field("label", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("allowed", FieldType.nullable(new ArrowType.Int(64, true)), null))),
                List.of(List.of("two", "four"), List.of(2L, 4L)));
        String expression = "{\"node\":\"in\",\"expression\":" + column("n")
                + ",\"set\":{\"kind\":\"external\",\"batch_index\":1,\"column_index\":1,"
                + "\"column_name\":\"allowed\"},\"negated\":false}";

        PushdownFilters filters = PushdownFiltersDecoder.decode(
                batch(snapshot(predicate("query", "required", expression))), outputSchema,
                List.of(firstBatch, secondBatch), PushdownFiltersDecoder.Capabilities.core());
        try (VectorSchemaRoot root = VectorSchemaRoot.create(outputSchema, Allocators.root())) {
            root.allocateNew();
            VectorScalarCodec.write(root.getVector(0), 0, 2L);
            VectorScalarCodec.write(root.getVector(0), 1, 3L);
            VectorScalarCodec.write(root.getVector(0), 2, 4L);
            root.getVector(0).setValueCount(3);
            root.setRowCount(3);
            assertArrayEquals(new boolean[] {true, false, true}, filters.evaluate(root));
        }

        String wrongName = expression.replace("\"column_name\":\"allowed\"",
                "\"column_name\":\"wrong\"");
        assertThrows(FilterV2Exception.class, () -> PushdownFiltersDecoder.decode(
                batch(snapshot(predicate("query", "required", wrongName))), outputSchema,
                List.of(firstBatch, secondBatch), PushdownFiltersDecoder.Capabilities.core()));
    }

    @Test
    void authoritativeOutputSchemaRequiresMatchingIndexesAndNames() {
        Schema outputSchema = new Schema(List.of(
                new Field("actual", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        assertThrows(FilterV2Exception.class, () -> PushdownFiltersDecoder.decode(
                batch(snapshot(predicate("query", "required", isNull("wrong")))), outputSchema,
                List.of(), PushdownFiltersDecoder.Capabilities.core()));

        String outOfRange = "{\"node\":\"is_null\",\"expression\":"
                + column(1, "actual") + ",\"negated\":false}";
        assertThrows(FilterV2Exception.class, () -> PushdownFiltersDecoder.decode(
                batch(snapshot(predicate("query", "required", outOfRange))), outputSchema,
                List.of(), PushdownFiltersDecoder.Capabilities.core()));

        String wrongNestedName = "{\"node\":\"is_null\",\"expression\":{\"node\":\"field_ref\","
                + "\"expression\":" + column("outer")
                + ",\"field_index\":0,\"field_name\":\"wrong\"},\"negated\":false}";
        assertThrows(FilterV2Exception.class, () -> PushdownFiltersDecoder.decode(
                batch(snapshot(predicate("query", "required", wrongNestedName))), NESTED_SCHEMA,
                List.of(), PushdownFiltersDecoder.Capabilities.core()));
    }

    @Test
    void rejectsUnknownArrowExtensionsWhereSemanticsAreUsed() {
        Field unknownValue = new Field("value_0", new FieldType(true, new ArrowType.Binary(), null,
                Map.of("ARROW:extension:name", "example.unknown")), null);
        String comparison = "{\"node\":\"comparison\",\"op\":\"eq\",\"left\":"
                + column("n") + ",\"right\":{\"node\":\"literal\",\"value_ref\":0}}";
        assertThrows(FilterV2Exception.class, () -> decode(
                batch(snapshot(predicate("query", "required", comparison)),
                        List.of(unknownValue), List.of(new byte[] {1})), INT_SCHEMA));

        Field unknownColumn = new Field("n", new FieldType(true, new ArrowType.Binary(), null,
                Map.of("ARROW:extension:name", "example.unknown")), null);
        assertThrows(FilterV2Exception.class, () -> decode(
                batch(snapshot(predicate("query", "required", isNull("n")))),
                new Schema(List.of(unknownColumn))));
    }

    @Test
    void noneContextRejectsContextDependentCasts() {
        Field type = new Field("type_0", FieldType.nullable(new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)), null);
        Schema strings = new Schema(List.of(
                new Field("s", FieldType.nullable(new ArrowType.Utf8()), null)));
        String cast = "{\"node\":\"is_null\",\"expression\":{\"node\":\"cast\","
                + "\"expression\":" + column("s")
                + ",\"type_ref\":0},\"negated\":false}";
        assertThrows(FilterV2Exception.class, () -> decode(
                batch(snapshot(predicate("query", "required", cast)),
                        List.of(type), Arrays.asList((Object) null)), strings));
    }

    @Test
    void rootTypeAndStandardFunctionBindingAreValidatedAtDecode() {
        PushdownFilters booleanRoot = decode(
                batch(snapshot(predicate("query", "required", column("flag")))), BOOL_SCHEMA);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(BOOL_SCHEMA, Allocators.root())) {
            root.allocateNew();
            VectorScalarCodec.write(root.getVector(0), 0, true);
            VectorScalarCodec.write(root.getVector(0), 1, false);
            root.getVector(0).setNull(2);
            root.getVector(0).setValueCount(3);
            root.setRowCount(3);
            assertArrayEquals(new boolean[] {true, false, false}, booleanRoot.evaluate(root));
        }

        assertThrows(FilterV2Exception.class, () -> decode(
                batch(snapshot(predicate("query", "required", column("n")))), INT_SCHEMA));

        Field text = new Field("value_0", FieldType.nullable(new ArrowType.Utf8()), null);
        String badCall = "{\"node\":\"call\",\"function\":\"starts_with\","
                + "\"arguments\":[{\"node\":\"literal\",\"value_ref\":0}]}";
        assertThrows(FilterV2Exception.class, () -> decode(
                batch(snapshot(predicate("hint", "advisory", badCall)),
                        List.of(text), List.of("abc")), new Schema(List.of())));
    }

    @Test
    void arithmeticAndMembershipTypesBindBeforeStateIsInstalled() {
        Schema mixed = new Schema(List.of(
                new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("s", FieldType.nullable(new ArrowType.Utf8()), null)));
        String arithmetic = "{\"node\":\"comparison\",\"op\":\"eq\",\"left\":"
                + "{\"node\":\"arithmetic\",\"op\":\"add\",\"left\":" + column("n")
                + ",\"right\":" + column(1, "s") + "},\"right\":" + column("n") + "}";
        assertThrows(FilterV2Exception.class, () -> decode(
                batch(snapshot(predicate("hint", "advisory", arithmetic))), mixed));

        Field strings = new Field("value_0", FieldType.nullable(new ArrowType.List()),
                List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        String membership = "{\"node\":\"in\",\"expression\":" + column("n")
                + ",\"set\":{\"kind\":\"literal\",\"value_ref\":0},\"negated\":false}";
        assertThrows(FilterV2Exception.class, () -> decode(
                batch(snapshot(predicate("hint", "advisory", membership)),
                        List.of(strings), List.of(List.of("one"))), INT_SCHEMA));
    }

    @Test
    void identitiesAreStrictEvenForAdvisoryPredicates() {
        String unknown = "{\"node\":\"call\",\"function\":{\"namespace\":\"example.filters\","
                + "\"name\":\"custom\",\"version\":1},\"arguments\":["
                + column("n") + "," + column("n") + "]}";
        PushdownFiltersDecoder.Capabilities advertised = new PushdownFiltersDecoder.Capabilities(
                java.util.Set.of(new FilterIdentity("example.filters", "custom", 1)),
                java.util.Set.of(), Map.of());
        assertThrows(FilterV2Exception.class, () -> PushdownFiltersDecoder.decode(
                batch(snapshot(predicate("hint", "advisory", unknown))), INT_SCHEMA,
                List.of(), advertised));

        String malformed = unknown.replace("example.filters", "Example.Filters");
        assertThrows(FilterV2Exception.class, () -> PushdownFiltersDecoder.decode(
                batch(snapshot(predicate("hint", "advisory", malformed))), INT_SCHEMA,
                List.of(), advertised));
        assertThrows(IllegalArgumentException.class,
                () -> new FilterIdentity("Example.Filters", "custom", 1));
        assertThrows(IllegalArgumentException.class,
                () -> new FilterIdentity("example.filters", "custom", 0));
    }

    @Test
    void spatialExpressionsRenderWkbAsGeometry() {
        Field geometry = new Field("geom", new FieldType(true, new ArrowType.Binary(), null,
                Map.of("ARROW:extension:name", "geoarrow.wkb",
                        "ARROW:extension:metadata", "{}")), null);
        Field literal = new Field("value_0", new FieldType(true, new ArrowType.Binary(), null,
                Map.of("ARROW:extension:name", "geoarrow.wkb",
                        "ARROW:extension:metadata", "{}")), null);
        String expression = "{\"node\":\"call\",\"function\":{"
                + "\"namespace\":\"duckdb.spatial\",\"name\":\"intersects_extent\",\"version\":1},"
                + "\"arguments\":[" + column("geom")
                + ",{\"node\":\"literal\",\"value_ref\":0}]}";
        PushdownFiltersDecoder.Capabilities capabilities = new PushdownFiltersDecoder.Capabilities(
                java.util.Set.of(new FilterIdentity("duckdb.spatial", "intersects_extent", 1)),
                java.util.Set.of(), Map.of());
        PushdownFilters filters = PushdownFiltersDecoder.decode(
                batch(snapshot(predicate("spatial", "required", expression)),
                        List.of(literal), List.of(new byte[] {1, 2, (byte) 0xff})),
                new Schema(List.of(geometry)), List.of(), capabilities);

        assertEquals(List.of("st_intersects_extent(\"geom\", "
                        + "ST_GeomFromHEXWKB('0102ff'))"),
                filters.expressionPredicates());
    }

    @Test
    void configuredLimitsAndFullUint64RevisionsAreEnforced() {
        String longId = "x".repeat(129);
        assertThrows(FilterV2Exception.class, () -> decode(batch(snapshot(
                predicate(longId, "required", isNull("n")))), INT_SCHEMA));

        Field bool = new Field("value_0", FieldType.nullable(new ArrowType.Bool()), null);
        String tooDeep = "{\"node\":\"literal\",\"value_ref\":0}";
        for (int i = 0; i < 65; i++) {
            tooDeep = "{\"node\":\"not\",\"expression\":" + tooDeep + "}";
        }
        String deepExpression = tooDeep;
        assertThrows(FilterV2Exception.class, () -> decode(
                batch(snapshot(predicate("query", "required", deepExpression)),
                        List.of(bool), List.of(true)), new Schema(List.of())));

        PushdownFilters state = decode(batch(snapshot(
                predicate("dynamic", "advisory", isNull("n")))), INT_SCHEMA);
        PushdownFilters maximum = state.applyDelta(batch(delta(
                "{\"operation\":\"remove\",\"id\":\"dynamic\","
                        + "\"revision\":18446744073709551615}")));
        assertEquals("18446744073709551615",
                Long.toUnsignedString(maximum.revisions().get("dynamic")));
        PushdownFilters stale = maximum.applyDelta(batch(delta(
                "{\"operation\":\"remove\",\"id\":\"dynamic\","
                        + "\"revision\":18446744073709551614}")));
        assertEquals(maximum.revisions(), stale.revisions());
    }

    @Test
    void malformedApplicableDeltaRollsBackEveryUpdate() {
        PushdownFilters state = decode(batch(snapshot(
                predicate("dynamic", "advisory", isNull("n")))), INT_SCHEMA);
        String malformed = delta(
                "{\"operation\":\"remove\",\"id\":\"dynamic\",\"revision\":1}",
                "{\"operation\":\"upsert\",\"id\":\"other\",\"revision\":1,"
                        + "\"mode\":\"advisory\",\"source\":\"join\","
                        + "\"expression\":{\"node\":\"future_node\"}}");
        assertThrows(FilterV2Exception.class, () -> state.applyDelta(batch(malformed)));
        assertEquals(0L, state.revisions().get("dynamic"));
        assertEquals(1, state.predicates().size());
    }

    @SuppressWarnings("removal")
    @Test
    void unsafeDecodingOverloadsRejectNonemptyV2Payloads() {
        byte[] bytes = batch(snapshot(predicate("query", "required", isNull("n"))));
        assertThrows(FilterV2Exception.class, () -> PushdownFiltersDecoder.decode(bytes));
        assertThrows(FilterV2Exception.class, () -> PushdownFiltersDecoder.decode(bytes, List.of()));
        assertThrows(FilterV2Exception.class, () -> FilterApplier.from(bytes, List.of()));
    }

    private static String snapshot(String... predicates) {
        return "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\","
                + "\"kind\":\"snapshot\",\"predicates\":[" + String.join(",", predicates) + "]}";
    }

    private static String delta(String... updates) {
        return "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\","
                + "\"kind\":\"delta\",\"updates\":[" + String.join(",", updates) + "]}";
    }

    private static String predicate(String id, String mode, String expression) {
        return "{\"id\":\"" + id + "\",\"revision\":0,\"mode\":\"" + mode
                + "\",\"source\":\"query\",\"expression\":" + expression + "}";
    }

    private static String isNull(String name) {
        return "{\"node\":\"is_null\",\"expression\":" + column(name) + ",\"negated\":false}";
    }

    private static String column(String name) {
        return column(0, name);
    }

    private static String column(int index, String name) {
        return "{\"node\":\"column_ref\",\"column_index\":" + index
                + ",\"column_name\":\"" + name + "\"}";
    }

    private static byte[] batch(String json) {
        return batch(json, List.of(), List.of());
    }

    private static byte[] batch(String json, List<Field> payloadFields, List<?> payloadValues) {
        if (payloadFields.size() != payloadValues.size()) {
            throw new IllegalArgumentException("one value is required for each payload field");
        }
        Field spec = new Field("filter_spec", FieldType.notNullable(new ArrowType.Utf8()), null);
        List<Field> fields = new ArrayList<>();
        fields.add(spec);
        fields.addAll(payloadFields);
        Schema schema = new Schema(fields, Map.of(
                "vgi_filter_encoding", "vgi.filters.v2",
                "vgi_filter_version", "2",
                "vgi_evaluation_context", "vgi.none.v1"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            VectorScalarCodec.write(root.getVector(0), 0, json);
            for (int i = 0; i < payloadValues.size(); i++) {
                VectorScalarCodec.write(root.getVector(i + 1), 0, payloadValues.get(i));
            }
            for (var vector : root.getFieldVectors()) vector.setValueCount(1);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static PushdownFilters decode(byte[] data, Schema outputSchema) {
        return PushdownFiltersDecoder.decode(data, outputSchema, List.of(),
                PushdownFiltersDecoder.Capabilities.core());
    }

    private static byte[] externalBatch(Schema schema, List<List<?>> columns) {
        if (schema.getFields().size() != columns.size()) {
            throw new IllegalArgumentException("one value list is required for each field");
        }
        int rowCount = columns.isEmpty() ? 0 : columns.get(0).size();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            for (int column = 0; column < columns.size(); column++) {
                if (columns.get(column).size() != rowCount) {
                    throw new IllegalArgumentException("all columns must have the same row count");
                }
                for (int row = 0; row < rowCount; row++) {
                    VectorScalarCodec.write(root.getVector(column), row, columns.get(column).get(row));
                }
                root.getVector(column).setValueCount(rowCount);
            }
            root.setRowCount(rowCount);
            return BatchUtil.writeSingleBatch(root);
        }
    }
}
