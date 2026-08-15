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
 *   <li>column 0, {@code filter_spec}: a UTF-8 JSON <em>array</em> of filter
 *       nodes. Its field carries the metadata {@code vgi_filter_version = "1"};
 *       a worker rejects the payload outright without it.</li>
 *   <li>columns {@code _val_0} … {@code _val_N-1}: the typed constants. A node's
 *       {@code value_ref: N} resolves to batch column {@code N + 1} — the JSON
 *       stays type-agnostic and the constants keep their Arrow types.</li>
 * </ul>
 *
 * <p>{@code join_keys} predicates are the exception: their values do
 * <em>not</em> occupy a {@code _val_N} column but ride as separate
 * single-column batches, matched to their node by column name. Both artefacts
 * come back together in {@link EncodedPushdownFilters}.
 *
 * <p><strong>Column indices are projected positions.</strong> Every node
 * carries {@code column_name} and {@code column_index}, and the index is the
 * column's position in the <em>projected</em> column list — not in the base
 * schema. A worker applies a filter by index, so a base-schema index filters
 * the wrong column with no error. Build columns through
 * {@link ProjectedColumns} rather than counting by hand; see
 * {@link ProjectedColumn} for the full argument.
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

    /** The only filter-spec version any VGI worker accepts today. */
    public static final String FILTER_VERSION = "1";

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
        ArrayNode specs = JSON.createArrayNode();
        List<ScalarValue> values = new ArrayList<>();
        List<JoinKeyColumn> joinKeyColumns = new ArrayList<>();

        for (int i = 0; i < columns.size(); i++) {
            specs.add(node(columns.get(i), predicates.get(i), values, joinKeyColumns));
        }

        String filterSpec = specs.toString();

        // Field 0 carries the version metadata; without it every decoder —
        // Java, Python, C++ — rejects the batch before looking at the JSON.
        Map<String, String> versionMetadata = new LinkedHashMap<>();
        versionMetadata.put("vgi_filter_version", FILTER_VERSION);
        List<Field> fields = new ArrayList<>(values.size() + 1);
        fields.add(new Field("filter_spec",
                new FieldType(true, new ArrowType.Utf8(), null, versionMetadata), null));
        for (int i = 0; i < values.size(); i++) {
            fields.add(values.get(i).field("_val_" + i));
        }

        byte[] filterBytes;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), Allocators.root())) {
            root.allocateNew();
            ScalarValue.of(filterSpec).write(root.getVector("filter_spec"), 0);
            for (int i = 0; i < values.size(); i++) {
                values.get(i).write(root.getVector("_val_" + i), 0);
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
    private ObjectNode node(ProjectedColumn column, FilterPredicate predicate,
                            List<ScalarValue> values, List<JoinKeyColumn> joinKeyColumns) {
        ObjectNode obj = JSON.createObjectNode();
        obj.put("column_name", column.name());
        obj.put("column_index", column.projectedIndex());

        switch (predicate) {
            case FilterPredicate.Compare c -> {
                obj.put("type", "constant");
                obj.put("op", c.op().wireToken());
                obj.put("value_ref", values.size());
                values.add(c.value());
            }
            case FilterPredicate.IsNull ignored -> obj.put("type", "is_null");
            case FilterPredicate.IsNotNull ignored -> obj.put("type", "is_not_null");
            case FilterPredicate.And a -> {
                obj.put("type", "and");
                obj.set("children", children(column, a.children(), values, joinKeyColumns));
            }
            case FilterPredicate.Or o -> {
                obj.put("type", "or");
                obj.set("children", children(column, o.children(), values, joinKeyColumns));
            }
            case FilterPredicate.StructField s -> {
                obj.put("type", "struct");
                obj.put("child_index", s.childIndex());
                obj.put("child_name", s.childName());
                obj.set("child_filter", node(column, s.childFilter(), values, joinKeyColumns));
            }
            case FilterPredicate.JoinKeys j -> {
                obj.put("type", "join_keys");
                // Matched back to its batch by name, so the batch's single
                // column is named after this filter's column.
                obj.put("keys_column", column.name());
                joinKeyColumns.add(new JoinKeyColumn(column.name(), j.type(), j.values()));
            }
        }
        return obj;
    }

    private ArrayNode children(ProjectedColumn column, List<FilterPredicate> children,
                               List<ScalarValue> values, List<JoinKeyColumn> joinKeyColumns) {
        ArrayNode arr = JSON.createArrayNode();
        for (FilterPredicate child : children) {
            arr.add(node(column, child, values, joinKeyColumns));
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
