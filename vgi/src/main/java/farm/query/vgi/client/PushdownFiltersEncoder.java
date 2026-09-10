// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encode filter predicates into the {@code InitRequest.pushdown_filters} (and
 * {@code InitRequest.join_keys}) wire form.
 *
 * <p>The inverse of the worker-side {@code PushdownFiltersDecoder}, and a
 * port of the C++ extension's {@code VgiSerializeFilters} — the authoritative
 * producer. The wire form is one single-row record batch:
 *
 * <ul>
 *   <li>column 0, {@code filter_spec}: the v2 snapshot JSON document.</li>
 *   <li>columns {@code value_0} … {@code value_N-1}: the typed constants. A node's
 *       {@code value_ref: N} resolves to batch column {@code N + 1} — the JSON
 *       stays type-agnostic and the constants keep their Arrow types.</li>
 * </ul>
 *
 * <p>{@code join_keys} predicates are the exception: their values do
 * <em>not</em> occupy a {@code value_N} column but ride as separate
 * single-column batches, matched to their node by column name. Both artefacts
 * come back together in {@link EncodedPushdownFilters}.
 *
 * <p>Column indices address the complete unprojected bind-output schema. Build
 * references through {@link ProjectedColumns} constructed from that schema.
 *
 * <pre>{@code
 * ProjectedColumns cols = ProjectedColumns.of(List.of("n", "name"));
 * EncodedPushdownFilters f = PushdownFiltersEncoder.builder()
 *         .filter(cols.column("n"), FilterPredicate.and(
 *                 FilterPredicate.ge(5L), FilterPredicate.lt(100L)))
 *         .filter(cols.column("name"), FilterPredicate.joinKeys(List.of("a", "b")))
 *         .encode();
 *
 * new InitRequest(..., f.pushdownFilters(), f.joinKeys(), ...);
 * }</pre>
 *
 * <p>Instances are mutable builders and are not thread-safe; build one per scan.
 */
public final class PushdownFiltersEncoder {

    /** The only filter-spec version emitted by this VGI 2.0 SDK. */
    public static final String FILTER_VERSION = "2";

    /** Schema-level version marker the C++ extension stamps on each join-key batch. */
    private static final String JOIN_KEYS_VERSION = "2";

    private static final ObjectMapper JSON = new ObjectMapper();

    private final List<ProjectedColumn> columns = new ArrayList<>();
    private final List<FilterPredicate> predicates = new ArrayList<>();

    private PushdownFiltersEncoder() {}

    /**
     * Start a new filter set.
     *
     * @return a fresh, empty encoder
     */
    public static PushdownFiltersEncoder builder() {
        return new PushdownFiltersEncoder();
    }

    /**
     * Add one column-rooted filter. Multiple filters are implicitly ANDed by
     * the worker, exactly as DuckDB's own filter set is.
     *
     * @param column    the column the predicate applies to — its index must be a
     *                  <em>projected</em> position (see {@link ProjectedColumn})
     * @param predicate the predicate, built from {@link FilterPredicate}'s factories
     * @return this encoder
     */
    public PushdownFiltersEncoder filter(ProjectedColumn column, FilterPredicate predicate) {
        if (column == null) throw new IllegalArgumentException("filter requires a column");
        if (predicate == null) throw new IllegalArgumentException("filter requires a predicate");
        columns.add(column);
        predicates.add(predicate);
        return this;
    }

    /**
     * Serialise the accumulated filters.
     *
     * @return the filter batch plus one batch per join-key predicate
     */
    public EncodedPushdownFilters encode() {
        ObjectNode document = JSON.createObjectNode();
        document.put("encoding", "vgi.filters.v2");
        document.put("semantics", "vgi.duckdb.standard.v1");
        document.put("kind", "snapshot");
        ArrayNode specs = document.putArray("predicates");
        List<ScalarValue> values = new ArrayList<>();
        List<JoinKeyColumn> joinKeyColumns = new ArrayList<>();

        for (int i = 0; i < columns.size(); i++) {
            ObjectNode predicate = specs.addObject();
            predicate.put("id", "p" + i);
            predicate.put("revision", 0);
            predicate.put("mode", "required");
            predicate.put("source", "query");
            predicate.set("expression", node(columnRef(columns.get(i)), predicates.get(i),
                    values, joinKeyColumns));
        }

        String filterSpec = document.toString();

        List<Field> fields = new ArrayList<>(values.size() + 1);
        fields.add(new Field("filter_spec",
                new FieldType(false, new ArrowType.Utf8(), null), null));
        for (int i = 0; i < values.size(); i++) {
            fields.add(values.get(i).field("value_" + i));
        }

        byte[] filterBytes;
        Schema schema = new Schema(fields, Map.of(
                "vgi_filter_encoding", "vgi.filters.v2",
                "vgi_filter_version", FILTER_VERSION,
                "vgi_evaluation_context", "vgi.none.v1"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            ScalarValue.of(filterSpec).write(root.getVector("filter_spec"), 0);
            for (int i = 0; i < values.size(); i++) {
                values.get(i).write(root.getVector("value_" + i), 0);
            }
            for (FieldVector v : root.getFieldVectors()) v.setValueCount(1);
            root.setRowCount(1);
            filterBytes = BatchUtil.writeSingleBatch(root);
        }

        List<byte[]> joinKeys = new ArrayList<>(joinKeyColumns.size());
        for (JoinKeyColumn kc : joinKeyColumns) joinKeys.add(encodeJoinKeys(kc));
        return new EncodedPushdownFilters(filterBytes, List.copyOf(joinKeys));
    }

    /**
     * Build one filter node. {@code column_name} / {@code column_index} are
     * repeated on every node, including children — the C++ serializer copies
     * the parent's column identity down the tree and the decoders read it from
     * each node independently.
     */
    private ObjectNode node(ObjectNode input, FilterPredicate predicate,
                            List<ScalarValue> values, List<JoinKeyColumn> joinKeyColumns) {
        ObjectNode obj = JSON.createObjectNode();

        switch (predicate) {
            case FilterPredicate.Compare c -> {
                obj.put("node", "comparison");
                obj.put("op", c.op().wireToken());
                obj.set("left", input);
                ObjectNode literal = obj.putObject("right");
                literal.put("node", "literal");
                literal.put("value_ref", values.size());
                values.add(c.value());
            }
            case FilterPredicate.IsNull ignored -> {
                obj.put("node", "is_null");
                obj.set("expression", input);
                obj.put("negated", false);
            }
            case FilterPredicate.IsNotNull ignored -> {
                obj.put("node", "is_null");
                obj.set("expression", input);
                obj.put("negated", true);
            }
            case FilterPredicate.And a -> {
                obj.put("node", "and");
                obj.set("children", children(input, a.children(), values, joinKeyColumns));
            }
            case FilterPredicate.Or o -> {
                obj.put("node", "or");
                obj.set("children", children(input, o.children(), values, joinKeyColumns));
            }
            case FilterPredicate.StructField s -> {
                ObjectNode field = JSON.createObjectNode();
                field.put("node", "field_ref");
                field.set("expression", input);
                field.put("field_index", s.childIndex());
                field.put("field_name", s.childName());
                return node(field, s.childFilter(), values, joinKeyColumns);
            }
            case FilterPredicate.JoinKeys j -> {
                obj.put("node", "in");
                obj.set("expression", input);
                ObjectNode set = obj.putObject("set");
                set.put("kind", "external");
                set.put("batch_index", joinKeyColumns.size());
                set.put("column_index", 0);
                String keyName = "key";
                set.put("column_name", keyName);
                obj.put("negated", false);
                joinKeyColumns.add(new JoinKeyColumn(keyName, j.type(), j.values()));
            }
        }
        return obj;
    }

    private ObjectNode columnRef(ProjectedColumn column) {
        ObjectNode ref = JSON.createObjectNode();
        ref.put("node", "column_ref");
        ref.put("column_index", column.bindIndex());
        ref.put("column_name", column.name());
        return ref;
    }

    private ArrayNode children(ObjectNode input, List<FilterPredicate> children,
                               List<ScalarValue> values, List<JoinKeyColumn> joinKeyColumns) {
        ArrayNode arr = JSON.createArrayNode();
        for (FilterPredicate child : children) {
            arr.add(node(input.deepCopy(), child, values, joinKeyColumns));
        }
        return arr;
    }

    private static byte[] encodeJoinKeys(JoinKeyColumn kc) {
        Field field = new Field(kc.name(), new FieldType(true, kc.type(), null), null);
        Schema schema = new Schema(List.of(field),
                Map.of("vgi_join_keys_version", JOIN_KEYS_VERSION));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            FieldVector v = root.getVector(kc.name());
            // Written straight through rather than one ScalarValue per key: a
            // runtime join-key push can carry tens of thousands of values.
            for (int i = 0; i < kc.values().size(); i++) {
                VectorScalarCodec.write(v, i, kc.values().get(i));
            }
            v.setValueCount(kc.values().size());
            root.setRowCount(kc.values().size());
            return BatchUtil.writeSingleBatch(root);
        }
    }

    /** One {@code join_keys} predicate's values, pending serialisation. */
    private record JoinKeyColumn(String name, ArrowType type, List<Object> values) {}
}
