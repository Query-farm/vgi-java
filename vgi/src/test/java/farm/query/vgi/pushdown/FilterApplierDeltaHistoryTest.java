// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.http.StateSerializer;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dynamic-filter history a {@link FilterApplier} carries across HTTP turns.
 *
 * <p>An HTTP stream keeps no parsed state between turns: the applier travels in
 * the continuation token ({@link StateSerializer}, the transport's own codec,
 * which is what these tests round-trip through) and each turn rebuilds its
 * filters from the init snapshot plus the deltas it carries. Carrying every
 * delta grew the token by one delta per tick and made turn {@code k} replay
 * {@code k} of them. These pin both halves of the compaction: the history is
 * bounded by the number of predicate IDs, and what it rebuilds is exactly the
 * state the applier had.
 */
final class FilterApplierDeltaHistoryTest {

    private static final Schema INT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    private static final Field VALUE_0 = new Field("value_0", FieldType.nullable(new ArrowType.Int(64, true)), null);
    private static final Field VALUE_1 = new Field("value_1", FieldType.nullable(new ArrowType.Int(64, true)), null);

    /** The minimal stream state that carries an applier, as a producer does. */
    public static final class Holder extends ProducerState {
        public FilterApplier applier;

        public Holder() {}

        Holder(FilterApplier applier) { this.applier = applier; }

        @Override public void produce(OutputCollector out, CallContext ctx) { out.finish(); }
    }

    @Test
    void aTighteningBoundKeepsOneDeltaAndTheTokenStopsGrowing() {
        FilterApplier applier = emptySnapshot();
        int tokenAfterFive = 0;
        for (int revision = 1; revision <= 200; revision++) {
            applier.applyDelta(delta(List.of(VALUE_0), List.of(10_000L - revision),
                    upsert("top_n:0", revision, "lt", 0)));
            if (revision == 5) tokenAfterFive = StateSerializer.serialize(new Holder(applier)).length;
        }
        assertEquals(1, applier.retainedDeltaCount());
        int tokenAfterTwoHundred = StateSerializer.serialize(new Holder(applier)).length;
        assertTrue(tokenAfterTwoHundred - tokenAfterFive < 16,
                "the carried history grew with the tick count: " + tokenAfterFive + " -> "
                        + tokenAfterTwoHundred + " bytes");

        FilterApplier rebuilt = roundTrip(applier);
        assertEquals("PushdownFilters([ConstantFilter(n < 9800)])", rebuilt.current().formatRepr());
        assertEquals(applier.current().revisions(), rebuilt.current().revisions());
    }

    @Test
    void aResentRevisionIsStaleAndAccumulatesNothing() {
        FilterApplier applier = emptySnapshot();
        applier.applyDelta(delta(List.of(VALUE_0), List.of(100_000L), upsert("top_n:0", 1, "lt", 0)));
        for (int i = 0; i < 20; i++) {
            applier.applyDelta(delta(List.of(VALUE_0), List.of(5L), upsert("top_n:0", 1, "lt", 0)));
        }
        assertEquals(1, applier.retainedDeltaCount());
        assertEquals("PushdownFilters([ConstantFilter(n < 100000)])", roundTrip(applier).current().formatRepr());
    }

    @Test
    void aRebuildKeepsThePredicateOrderTheApplierHad() {
        // {a:1, b:1} -> [a, b];  {remove a:2, b:1} -> [b];  {a:3, b:1} -> [b, a].
        // The compacted history is the deltas that first carried a:3 and b:1 --
        // the third and the first -- and replaying those alone yields [a, b].
        FilterApplier applier = emptySnapshot();
        applier.applyDelta(delta(List.of(VALUE_0, VALUE_1), List.of(100_000L, 5L),
                upsert("a", 1, "lt", 0), upsert("b", 1, "gt", 1)));
        applier.applyDelta(delta(List.of(VALUE_0), List.of(5L),
                remove("a", 2), upsert("b", 1, "gt", 0)));
        applier.applyDelta(delta(List.of(VALUE_0, VALUE_1), List.of(99_999L, 5L),
                upsert("a", 3, "lt", 0), upsert("b", 1, "gt", 1)));
        assertEquals(List.of("b", "a"), ids(applier.current()));
        assertEquals(2, applier.retainedDeltaCount());

        FilterApplier rebuilt = roundTrip(applier);
        assertEquals(List.of("b", "a"), ids(rebuilt.current()));
        assertEquals(applier.current().formatRepr(), rebuilt.current().formatRepr());
        assertEquals(applier.current().revisions(), rebuilt.current().revisions());
    }

    @Test
    void aTombstoneOutlivesItsCompactedUpsert() {
        FilterApplier applier = emptySnapshot();
        applier.applyDelta(delta(List.of(VALUE_0), List.of(100_000L), upsert("top_n:0", 1, "lt", 0)));
        applier.applyDelta(delta(List.of(), List.of(), remove("top_n:0", 2)));
        assertEquals(1, applier.retainedDeltaCount());

        FilterApplier rebuilt = roundTrip(applier);
        rebuilt.applyDelta(delta(List.of(VALUE_0), List.of(5L), upsert("top_n:0", 1, "lt", 0)));
        assertEquals("(none)", rebuilt.current().formatRepr(), "a stale upsert resurrected a removed predicate");
        assertEquals(2L, rebuilt.current().revisions().get("top_n:0"));
    }

    @Test
    void theRebuiltApplierFiltersRowsLikeTheLiveOne() {
        FilterApplier applier = emptySnapshot();
        for (int revision = 1; revision <= 30; revision++) {
            applier.applyDelta(delta(List.of(VALUE_0), List.of(100L - revision), upsert("top_n:0", revision, "lt", 0)));
        }
        FilterApplier rebuilt = roundTrip(applier);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(INT_SCHEMA, Allocators.root())) {
            root.allocateNew();
            for (int i = 0; i < 100; i++) VectorScalarCodec.write(root.getVector(0), i, (long) i);
            root.getVector(0).setValueCount(100);
            root.setRowCount(100);
            boolean[] live = applier.current().evaluate(root);
            boolean[] replayed = rebuilt.current().evaluate(root);
            assertEquals(70, count(live));
            assertEquals(count(live), count(replayed));
            for (int i = 0; i < live.length; i++) assertEquals(live[i], replayed[i], "row " + i);
        }
    }

    private static FilterApplier emptySnapshot() {
        return FilterApplier.from(batch(document("snapshot", "predicates", List.of()), List.of(), List.of()),
                List.of(), INT_SCHEMA, PushdownFiltersDecoder.Capabilities.core());
    }

    private static FilterApplier roundTrip(FilterApplier applier) {
        byte[] token = StateSerializer.serialize(new Holder(applier));
        return StateSerializer.deserialize(token, Holder.class).applier;
    }

    private static List<String> ids(PushdownFilters filters) {
        List<String> ids = new ArrayList<>();
        for (FilterPredicateV2 p : filters.predicates()) ids.add(p.id());
        return ids;
    }

    private static int count(boolean[] mask) {
        int n = 0;
        for (boolean b : mask) if (b) n++;
        return n;
    }

    private static String upsert(String id, long revision, String op, int valueRef) {
        return "{\"operation\":\"upsert\",\"id\":\"" + id + "\",\"revision\":" + revision
                + ",\"mode\":\"advisory\",\"source\":\"top_n\",\"expression\":{\"node\":\"comparison\","
                + "\"op\":\"" + op + "\",\"left\":{\"node\":\"column_ref\",\"column_index\":0,"
                + "\"column_name\":\"n\"},\"right\":{\"node\":\"literal\",\"value_ref\":" + valueRef + "}}}";
    }

    private static String remove(String id, long revision) {
        return "{\"operation\":\"remove\",\"id\":\"" + id + "\",\"revision\":" + revision + "}";
    }

    private static byte[] delta(List<Field> payloadFields, List<?> payloadValues, String... updates) {
        return batch(document("delta", "updates", List.of(updates)), payloadFields, payloadValues);
    }

    private static String document(String kind, String member, List<String> entries) {
        return "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\""
                + kind + "\",\"" + member + "\":[" + String.join(",", entries) + "]}";
    }

    private static byte[] batch(String json, List<Field> payloadFields, List<?> payloadValues) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("filter_spec", FieldType.notNullable(new ArrowType.Utf8()), null));
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
}
