// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Strict decoder and state transition implementation for VGI Filter Encoding v2. */
public final class PushdownFiltersDecoder {
    public static final String ENCODING = "vgi.filters.v2";
    public static final String VERSION = "2";
    public static final String SEMANTICS = "vgi.duckdb.standard.v1";
    public static final String NO_CONTEXT = "vgi.none.v1";
    public static final String DUCKDB_CONTEXT = "vgi.duckdb.session.v1";

    private static final int MAX_JSON_BYTES = 1 << 20;
    private static final int MAX_DEPTH = 64;
    private static final int MAX_NODES = 10_000;
    private static final int MAX_PREDICATES = 1_024;
    private static final int MAX_PREDICATE_IDS = 4_096;
    private static final int MAX_ARGUMENTS = 256;
    private static final int MAX_ID_BYTES = 128;
    private static final int MAX_PAYLOAD_BYTES = 16 << 20;
    private static final BigInteger UINT64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final Pattern PAYLOAD = Pattern.compile("(?:value|type|artifact)_(?:0|[1-9][0-9]*)");
    private static final Pattern NAMESPACE = Pattern.compile("[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9_]*)*");
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]*");
    private static final Field BOOLEAN_FIELD =
            new Field("result", FieldType.nullable(new ArrowType.Bool()), null);
    private static final Set<String> KNOWN_EXTENSIONS = Set.of(
            "arrow.bool8", "arrow.json", "arrow.uuid", "geoarrow.linestring",
            "geoarrow.multilinestring", "geoarrow.multipoint", "geoarrow.multipolygon",
            "geoarrow.point", "geoarrow.polygon", "geoarrow.wkb");
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private PushdownFiltersDecoder() {}

    /** Negotiated worker capabilities used to decide whether optional nodes are executable. */
    public record Capabilities(
            Set<FilterIdentity> extensionFunctions,
            Set<FilterIdentity> runtimeAlgorithms,
            Map<String, Set<String>> evaluationContexts) {
        public Capabilities {
            extensionFunctions = extensionFunctions == null ? Set.of() : Set.copyOf(extensionFunctions);
            runtimeAlgorithms = runtimeAlgorithms == null ? Set.of() : Set.copyOf(runtimeAlgorithms);
            evaluationContexts = evaluationContexts == null ? Map.of() : Map.copyOf(evaluationContexts);
        }

        public static Capabilities core() {
            return new Capabilities(Set.of(), Set.of(), Map.of());
        }
    }

    /**
     * @deprecated V2 references require the authoritative unprojected bind output schema.
     */
    @Deprecated(forRemoval = true)
    public static PushdownFilters decode(byte[] data) {
        if (data == null || data.length == 0) return PushdownFilters.empty();
        throw new FilterV2Exception("v2 filter decoding requires the authoritative bind output schema");
    }

    /**
     * @deprecated V2 references require the authoritative unprojected bind output schema.
     */
    @Deprecated(forRemoval = true)
    public static PushdownFilters decode(byte[] data, List<byte[]> joinKeysIpc) {
        if (data == null || data.length == 0) return PushdownFilters.empty();
        throw new FilterV2Exception("v2 filter decoding requires the authoritative bind output schema");
    }

    public static PushdownFilters decode(
            byte[] data, Schema outputSchema, List<byte[]> joinKeysIpc, Capabilities capabilities) {
        if (data == null || data.length == 0) return PushdownFilters.empty();
        return readBatch(data, root -> new Parser(root, outputSchema,
                readExternalBatches(joinKeysIpc), null, capabilities).parseSnapshot());
    }

    public static PushdownFilters applyDelta(PushdownFilters prior, byte[] data) {
        if (prior == null) throw new FilterV2Exception("a delta requires prior snapshot state");
        if (data == null || data.length == 0) return prior;
        return readBatch(data, root -> new Parser(root, prior.outputSchema(), prior.joinKeys(),
                prior, prior.capabilities()).parseDelta());
    }

    private static PushdownFilters readBatch(byte[] data,
                                              java.util.function.Function<VectorSchemaRoot, PushdownFilters> body) {
        try (var input = new ByteArrayInputStream(data);
             var reader = new ArrowStreamReader(input, Allocators.root())) {
            if (!reader.loadNextBatch()) throw new FilterV2Exception("filter IPC stream has no RecordBatch");
            PushdownFilters result = body.apply(reader.getVectorSchemaRoot());
            if (reader.loadNextBatch()) throw new FilterV2Exception("filter IPC stream must contain exactly one batch");
            return result;
        } catch (FilterV2Exception e) {
            throw e;
        } catch (Exception e) {
            throw new FilterV2Exception("failed to decode v2 filter batch", e);
        }
    }

    record ExternalBatch(Schema schema, List<List<Object>> columns) {}

    private static List<ExternalBatch> readExternalBatches(List<byte[]> batches) {
        if (batches == null || batches.isEmpty()) return List.of();
        List<ExternalBatch> result = new ArrayList<>();
        for (byte[] bytes : batches) {
            try (var input = new ByteArrayInputStream(bytes);
                 var reader = new ArrowStreamReader(input, Allocators.root())) {
                if (!reader.loadNextBatch()) throw new FilterV2Exception("external IN batch is empty");
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                List<List<Object>> columns = new ArrayList<>();
                for (FieldVector vector : root.getFieldVectors()) {
                    List<Object> values = new ArrayList<>(root.getRowCount());
                    for (int row = 0; row < root.getRowCount(); row++) {
                        values.add(VectorScalarCodec.read(vector, row));
                    }
                    columns.add(java.util.Collections.unmodifiableList(new ArrayList<>(values)));
                }
                if (reader.loadNextBatch()) {
                    throw new FilterV2Exception("external IN stream must contain exactly one batch");
                }
                result.add(new ExternalBatch(root.getSchema(), List.copyOf(columns)));
            } catch (FilterV2Exception e) {
                throw e;
            } catch (Exception e) {
                throw new FilterV2Exception("failed to decode external IN batch", e);
            }
        }
        return List.copyOf(result);
    }

    private static final class Parser {
        private final VectorSchemaRoot root;
        private final Schema outputSchema;
        private final List<ExternalBatch> joinKeys;
        private final PushdownFilters prior;
        private final Capabilities capabilities;
        private final FilterEvaluationContext context;
        private final JsonNode document;
        private int nodes;

        Parser(VectorSchemaRoot root, Schema outputSchema, List<ExternalBatch> joinKeys,
               PushdownFilters prior, Capabilities capabilities) {
            this.root = root;
            this.outputSchema = outputSchema;
            this.joinKeys = joinKeys;
            this.prior = prior;
            this.capabilities = capabilities;
            this.context = validateBatch();
            validateContextCapability();
            this.document = parseDocument();
        }

        PushdownFilters parseSnapshot() {
            List<JsonNode> entries = header("snapshot", "predicates");
            if (entries.size() > MAX_PREDICATES) fail("snapshot exceeds predicate limit");
            List<FilterPredicateV2> predicates = new ArrayList<>();
            Map<String, Long> revisions = new LinkedHashMap<>();
            Set<String> required = new HashSet<>();
            for (int i = 0; i < entries.size(); i++) {
                String where = "predicates[" + i + "]";
                JsonNode entry = object(entries.get(i), where);
                String predicateId = id(required(entry, "id", where), where);
                long revision = uint(required(entry, "revision", where), where + ".revision");
                if (revision != 0) fail("snapshot predicate revisions must be zero");
                if (revisions.putIfAbsent(predicateId, 0L) != null) {
                    fail("duplicate predicate ID " + predicateId);
                }
                PredicateMode mode = PredicateMode.fromWire(
                        text(required(entry, "mode", where), where + ".mode", true));
                if (mode == PredicateMode.REQUIRED) required.add(predicateId);
                FilterPredicateV2 predicate = predicate(entry, where, false);
                if (predicate != null) predicates.add(predicate);
            }
            return PushdownFilters.v2(predicates, revisions, required, context, outputSchema,
                    joinKeys, capabilities);
        }

        PushdownFilters parseDelta() {
            if (prior == null) fail("a delta requires prior snapshot state");
            if (!context.equals(prior.evaluationContext())) {
                fail("evaluation context changed within one scan");
            }
            List<JsonNode> updates = header("delta", "updates");
            Map<String, FilterPredicateV2> current = new LinkedHashMap<>();
            for (FilterPredicateV2 p : prior.predicates()) current.put(p.id(), p);
            Map<String, Long> revisions = new LinkedHashMap<>(prior.revisions());
            Set<String> seen = new HashSet<>();
            List<Delta> applicable = new ArrayList<>();
            for (int i = 0; i < updates.size(); i++) {
                String where = "updates[" + i + "]";
                JsonNode update = object(updates.get(i), where);
                String operation = text(required(update, "operation", where), where + ".operation", true);
                String id;
                long revision;
                FilterPredicateV2 value = null;
                if (operation.equals("remove")) {
                    keys(update, Set.of("operation", "id", "revision"), Set.of(), where);
                    id = id(required(update, "id", where), where);
                    revision = uint(required(update, "revision", where), where + ".revision");
                } else if (operation.equals("upsert")) {
                    keys(update, Set.of("operation", "id", "revision", "mode", "source", "expression"),
                            Set.of(), where);
                    id = id(required(update, "id", where), where);
                    revision = uint(required(update, "revision", where), where + ".revision");
                    PredicateMode mode = PredicateMode.fromWire(text(update.get("mode"), where + ".mode", true));
                    PredicateSource.fromWire(text(update.get("source"), where + ".source", true));
                    object(update.get("expression"), where + ".expression");
                    if (mode != PredicateMode.ADVISORY) fail("delta upserts must be advisory");
                } else {
                    fail(where + ".operation must be 'upsert' or 'remove'");
                    return null;
                }
                if (!seen.add(id)) fail("duplicate delta predicate ID " + id);
                if (prior.requiredIds().contains(id)) fail("delta targets required predicate " + id);
                Long priorRevision = revisions.get(id);
                if (priorRevision == null || Long.compareUnsigned(revision, priorRevision) > 0) {
                    if (operation.equals("upsert")) value = predicate(update, where, true);
                    applicable.add(new Delta(id, revision, value));
                }
            }
            Set<String> allIds = new HashSet<>(revisions.keySet());
            for (Delta delta : applicable) allIds.add(delta.id());
            if (allIds.size() > MAX_PREDICATE_IDS) fail("delta exceeds per-scan predicate-ID limit");
            for (Delta delta : applicable) {
                revisions.put(delta.id(), delta.revision());
                if (delta.value() == null) current.remove(delta.id());
                else current.put(delta.id(), delta.value());
            }
            return PushdownFilters.v2(new ArrayList<>(current.values()), revisions,
                    prior.requiredIds(), context, outputSchema, joinKeys, capabilities);
        }

        private record Delta(String id, long revision, FilterPredicateV2 value) {}

        private FilterEvaluationContext validateBatch() {
            if (root == null || root.getRowCount() != 1) fail("filter RecordBatch must contain exactly one row");
            if (root.getFieldVectors().isEmpty()) fail("filter RecordBatch has no filter_spec field");
            Field first = root.getSchema().getFields().getFirst();
            if (!first.getName().equals("filter_spec")
                    || !(first.getType() instanceof ArrowType.Utf8)
                    || first.isNullable()) {
                fail("first field must be filter_spec: utf8 not null");
            }
            if (root.getVector(0).isNull(0)) fail("filter_spec value must not be NULL");
            Set<String> names = new HashSet<>();
            for (int i = 0; i < root.getSchema().getFields().size(); i++) {
                Field field = root.getSchema().getFields().get(i);
                if (!names.add(field.getName())) fail("filter payload field names must be unique");
                if (i == 0) continue;
                if (!PAYLOAD.matcher(field.getName()).matches()) {
                    fail("noncanonical payload field name " + field.getName());
                }
                if (field.getName().startsWith("type_") && !root.getVector(i).isNull(0)) {
                    fail(field.getName() + " must contain a NULL value");
                }
            }
            long payloadBytes = 0;
            for (FieldVector vector : root.getFieldVectors()) payloadBytes += vector.getBufferSize();
            if (payloadBytes > MAX_PAYLOAD_BYTES) fail("filter payload exceeds 16 MiB");
            Map<String, String> metadata = root.getSchema().getCustomMetadata();
            if (!ENCODING.equals(metadata.get("vgi_filter_encoding"))) fail("unsupported filter encoding");
            if (!VERSION.equals(metadata.get("vgi_filter_version"))) fail("unsupported filter version");
            String profile = metadata.get("vgi_evaluation_context");
            if (NO_CONTEXT.equals(profile)) {
                for (String key : contextKeys()) if (metadata.containsKey(key)) {
                    fail("vgi.none.v1 forbids DuckDB session-context metadata");
                }
                return FilterEvaluationContext.none();
            }
            if (!DUCKDB_CONTEXT.equals(profile)) fail("unknown evaluation context " + profile);
            for (String key : contextKeys().subList(0, 5)) {
                if (!metadata.containsKey(key)) fail("incomplete DuckDB evaluation context; missing " + key);
            }
            Boolean ieee = contextBoolean(metadata, "vgi_ieee_floating_point_ops");
            Boolean integerDivision = contextBoolean(metadata, "vgi_integer_division");
            String fingerprint = metadata.get("vgi_context_provider_fingerprint");
            if (fingerprint != null && (fingerprint.isEmpty()
                    || fingerprint.getBytes(StandardCharsets.UTF_8).length > 256)) {
                fail("context provider fingerprint must be nonempty and at most 256 UTF-8 bytes");
            }
            return new FilterEvaluationContext(profile, metadata.get("vgi_time_zone"),
                    metadata.get("vgi_calendar"), metadata.get("vgi_default_collation"),
                    ieee, integerDivision, fingerprint);
        }

        private void validateContextCapability() {
            if (NO_CONTEXT.equals(context.profile())) return;
            Set<String> fingerprints = capabilities.evaluationContexts().get(context.profile());
            if (fingerprints == null) fail("evaluation context was not advertised: " + context.profile());
            if (context.providerFingerprint() != null && !fingerprints.contains(context.providerFingerprint())) {
                fail("evaluation-context provider fingerprint does not match an advertised capability");
            }
        }

        private JsonNode parseDocument() {
            Object raw = VectorScalarCodec.read(root.getVector(0), 0);
            if (!(raw instanceof String)) {
                fail("filter JSON must be UTF-8 and at most 1 MiB");
            }
            String json = (String) raw;
            if (json.getBytes(StandardCharsets.UTF_8).length > MAX_JSON_BYTES) {
                fail("filter JSON must be UTF-8 and at most 1 MiB");
            }
            try {
                return object(JSON.readTree(json), "filter document");
            } catch (Exception e) {
                throw new FilterV2Exception("invalid filter JSON", e);
            }
        }

        private List<JsonNode> header(String kind, String member) {
            keys(document, Set.of("encoding", "semantics", "kind", member), Set.of(), "filter document");
            if (!ENCODING.equals(text(document.get("encoding"), "encoding", true))) {
                fail("document encoding must be vgi.filters.v2");
            }
            if (!SEMANTICS.equals(text(document.get("semantics"), "semantics", true))) {
                fail("unsupported filter semantics");
            }
            if (!kind.equals(text(document.get("kind"), "kind", true))) fail("expected a " + kind + " document");
            return array(document.get(member), member);
        }

        private FilterPredicateV2 predicate(JsonNode value, String where, boolean update) {
            Set<String> required = new HashSet<>(Set.of("id", "revision", "mode", "source", "expression"));
            if (update) required.add("operation");
            keys(value, required, Set.of(), where);
            String id = id(value.get("id"), where);
            long revision = uint(value.get("revision"), where + ".revision");
            PredicateMode mode = PredicateMode.fromWire(text(value.get("mode"), where + ".mode", true));
            PredicateSource source = PredicateSource.fromWire(text(value.get("source"), where + ".source", true));
            FilterExpression expression = expression(object(value.get("expression"), where + ".expression"), 1, true);
            if (NO_CONTEXT.equals(context.profile()) && requiresContext(expression)) {
                fail("context-dependent expression requires vgi.duckdb.session.v1");
            }
            if (expression instanceof FilterExpression.RuntimeFilter && mode != PredicateMode.ADVISORY) {
                fail("runtime_filter predicates must be advisory");
            }
            if (!(expression instanceof FilterExpression.RuntimeFilter) && !isBoolean(expression)) {
                fail("predicate root must resolve to BOOLEAN");
            }
            return new FilterPredicateV2(id, revision, mode, source, expression);
        }

        private FilterExpression expression(JsonNode value, int depth, boolean rootNode) {
            if (depth > MAX_DEPTH) fail("expression exceeds nesting-depth limit");
            if (++nodes > MAX_NODES) fail("document exceeds expression-node limit");
            String node = text(required(value, "node", "expression"), "expression.node", true);
            return switch (node) {
                case "column_ref" -> columnRef(value);
                case "field_ref" -> fieldRef(value, depth);
                case "literal" -> literal(value);
                case "comparison" -> comparison(value, depth);
                case "and", "or" -> booleanExpression(value, depth, node.equals("and"));
                case "not" -> not(value, depth);
                case "is_null" -> isNull(value, depth);
                case "in" -> in(value, depth);
                case "cast" -> cast(value, depth);
                case "arithmetic" -> arithmetic(value, depth);
                case "negate" -> negate(value, depth);
                case "call" -> call(value, depth);
                case "runtime_filter" -> runtimeFilter(value, depth, rootNode);
                default -> {
                    fail("unknown expression node " + node);
                    yield null;
                }
            };
        }

        private FilterExpression columnRef(JsonNode value) {
            keys(value, Set.of("node", "column_index", "column_name"), Set.of(), "column_ref");
            long index = uint(value.get("column_index"), "column_ref.column_index");
            String name = text(value.get("column_name"), "column_ref.column_name", false);
            Field field = null;
            if (outputSchema != null) {
                if (outOfRange(index, outputSchema.getFields().size())) fail("column_ref index is out of range");
                field = outputSchema.getFields().get((int) index);
                validateExtensions(field);
                if (!field.getName().equals(name)) fail("column_ref name does not match its index");
            }
            return new FilterExpression.ColumnRef(index, name, field);
        }

        private FilterExpression fieldRef(JsonNode value, int depth) {
            keys(value, Set.of("node", "expression", "field_index", "field_name"), Set.of(), "field_ref");
            FilterExpression parent = expression(object(value.get("expression"), "field_ref.expression"), depth + 1, false);
            long index = uint(value.get("field_index"), "field_ref.field_index");
            String name = text(value.get("field_name"), "field_ref.field_name", false);
            Field parentField = expressionField(parent);
            if (parentField == null || !(parentField.getType() instanceof ArrowType.Struct)) {
                fail("field_ref input must resolve to a struct type");
            }
            if (outOfRange(index, parentField.getChildren().size())) fail("field_ref index is out of range");
            Field field = parentField.getChildren().get((int) index);
            if (!field.getName().equals(name)) fail("field_ref name does not match its index");
            return new FilterExpression.FieldRef(parent, index, name, field);
        }

        private FilterExpression literal(JsonNode value) {
            keys(value, Set.of("node", "value_ref"), Set.of(), "literal");
            Payload payload = payload("value", value.get("value_ref"));
            validateExtensions(payload.field());
            return new FilterExpression.Literal(payload.ref(), payload.field(), payload.value());
        }

        private FilterExpression comparison(JsonNode value, int depth) {
            keys(value, Set.of("node", "op", "left", "right"), Set.of(), "comparison");
            ComparisonOperator op;
            try {
                op = ComparisonOperator.fromWire(text(value.get("op"), "comparison.op", true));
            } catch (IllegalArgumentException e) {
                throw new FilterV2Exception(e.getMessage(), e);
            }
            FilterExpression left = expression(object(value.get("left"), "comparison.left"), depth + 1, false);
            FilterExpression right = expression(object(value.get("right"), "comparison.right"), depth + 1, false);
            validateComparable(left, right, "comparison operands");
            return new FilterExpression.Comparison(op, left, right);
        }

        private FilterExpression booleanExpression(JsonNode value, int depth, boolean and) {
            keys(value, Set.of("node", "children"), Set.of(), and ? "and" : "or");
            List<JsonNode> raw = array(value.get("children"), "children");
            if (raw.size() < 2) fail("and/or requires at least two children");
            List<FilterExpression> children = new ArrayList<>();
            for (JsonNode child : raw) {
                FilterExpression parsed = expression(object(child, "child expression"), depth + 1, false);
                if (!isBoolean(parsed)) fail("and/or children must resolve to BOOLEAN");
                children.add(parsed);
            }
            return new FilterExpression.BooleanExpression(and, List.copyOf(children));
        }

        private FilterExpression not(JsonNode value, int depth) {
            keys(value, Set.of("node", "expression"), Set.of(), "not");
            FilterExpression child = expression(object(value.get("expression"), "not.expression"), depth + 1, false);
            if (!isBoolean(child)) fail("not input must resolve to BOOLEAN");
            return new FilterExpression.Not(child);
        }

        private FilterExpression isNull(JsonNode value, int depth) {
            keys(value, Set.of("node", "expression", "negated"), Set.of(), "is_null");
            return new FilterExpression.IsNull(
                    expression(object(value.get("expression"), "is_null.expression"), depth + 1, false),
                    bool(value.get("negated"), "is_null.negated"));
        }

        private FilterExpression in(JsonNode value, int depth) {
            keys(value, Set.of("node", "expression", "set", "negated"), Set.of(), "in");
            FilterExpression input = expression(object(value.get("expression"), "in.expression"), depth + 1, false);
            JsonNode set = object(value.get("set"), "in.set");
            String kind = text(required(set, "kind", "in.set"), "in.set.kind", true);
            FilterExpression.ValueSet values;
            if (kind.equals("literal")) {
                keys(set, Set.of("kind", "value_ref"), Set.of(), "in.set");
                Payload payload = payload("value", set.get("value_ref"));
                validateExtensions(payload.field());
                if (!(payload.field().getType() instanceof ArrowType.List
                        || payload.field().getType() instanceof ArrowType.LargeList)) {
                    fail("literal IN payload must be a list scalar");
                }
                if (!(payload.value() instanceof List<?>)) fail("literal IN list must not be NULL");
                List<?> list = (List<?>) payload.value();
                values = new FilterExpression.LiteralSet(payload.ref(), payload.field(),
                        java.util.Collections.unmodifiableList(new ArrayList<>(list)));
            } else if (kind.equals("external")) {
                keys(set, Set.of("kind", "batch_index", "column_index", "column_name"), Set.of(), "in.set");
                long batchIndex = uint(set.get("batch_index"), "in.set.batch_index");
                long columnIndex = uint(set.get("column_index"), "in.set.column_index");
                String name = text(set.get("column_name"), "in.set.column_name", false);
                if (outOfRange(batchIndex, joinKeys.size())) fail("external IN batch index is unavailable");
                ExternalBatch batch = joinKeys.get((int) batchIndex);
                if (outOfRange(columnIndex, batch.schema().getFields().size())) {
                    fail("external IN column index is out of range");
                }
                Field field = batch.schema().getFields().get((int) columnIndex);
                if (!field.getName().equals(name)) fail("external IN column name does not match its index");
                validateExtensions(field);
                values = new FilterExpression.ExternalSet(batchIndex, columnIndex, name, field,
                        batch.columns().get((int) columnIndex));
            } else {
                fail("unknown IN set kind " + kind);
                return null;
            }
            validateMembership(input, values);
            return new FilterExpression.In(input, values, bool(value.get("negated"), "in.negated"));
        }

        private FilterExpression cast(JsonNode value, int depth) {
            keys(value, Set.of("node", "expression", "type_ref"), Set.of(), "cast");
            Payload payload = payload("type", value.get("type_ref"));
            validateExtensions(payload.field());
            if (payload.value() != null) fail("cast type payload must contain NULL");
            return new FilterExpression.Cast(
                    expression(object(value.get("expression"), "cast.expression"), depth + 1, false),
                    payload.ref(), payload.field());
        }

        private FilterExpression arithmetic(JsonNode value, int depth) {
            keys(value, Set.of("node", "op", "left", "right"), Set.of(), "arithmetic");
            FilterExpression.ArithmeticOperator op;
            try {
                op = FilterExpression.ArithmeticOperator.valueOf(
                        text(value.get("op"), "arithmetic.op", true).toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new FilterV2Exception("unknown arithmetic operator", e);
            }
            FilterExpression left = expression(object(value.get("left"), "arithmetic.left"), depth + 1, false);
            FilterExpression right = expression(object(value.get("right"), "arithmetic.right"), depth + 1, false);
            requireNumeric(left, "arithmetic left operand");
            requireNumeric(right, "arithmetic right operand");
            return new FilterExpression.Arithmetic(op, left, right);
        }

        private FilterExpression negate(JsonNode value, int depth) {
            keys(value, Set.of("node", "expression"), Set.of(), "negate");
            FilterExpression expression = expression(
                    object(value.get("expression"), "negate.expression"), depth + 1, false);
            requireNumeric(expression, "negate operand");
            return new FilterExpression.Negate(expression);
        }

        @SuppressWarnings("unchecked")
        private FilterExpression call(JsonNode value, int depth) {
            keys(value, Set.of("node", "function", "arguments"), Set.of("options"), "call");
            Object function;
            JsonNode rawFunction = value.get("function");
            if (rawFunction.isTextual()) {
                try {
                    function = FilterExpression.StandardFunction.valueOf(rawFunction.textValue().toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new FilterV2Exception(
                            "unknown standard filter function " + rawFunction.textValue(), e);
                }
                if (value.has("options")) fail("standard filter functions do not accept options");
            } else {
                FilterIdentity identity = identity(rawFunction, "call.function");
                if (!identity.equals(new FilterIdentity("duckdb.spatial", "intersects_extent", 1))) {
                    fail("unknown extension filter function " + identity);
                }
                if (!capabilities.extensionFunctions().contains(identity)) {
                    fail("extension filter function was not advertised: " + identity);
                }
                function = identity;
            }
            List<JsonNode> rawArgs = array(value.get("arguments"), "call.arguments");
            if (rawArgs.size() > MAX_ARGUMENTS) fail("call exceeds argument-count limit");
            List<FilterExpression> arguments = new ArrayList<>();
            for (JsonNode argument : rawArgs) {
                arguments.add(expression(object(argument, "call argument"), depth + 1, false));
            }
            Map<String, Object> options = value.has("options")
                    ? JSON.convertValue(object(value.get("options"), "call.options"), Map.class) : null;
            if (options != null && !options.isEmpty()) fail("extension filter function does not accept options");
            validateCall(function, arguments);
            return new FilterExpression.Call(function, List.copyOf(arguments), options);
        }

        private FilterExpression runtimeFilter(JsonNode value, int depth, boolean rootNode) {
            keys(value, Set.of("node", "algorithm", "input", "artifact_ref", "null_handling"),
                    Set.of(), "runtime_filter");
            if (!rootNode) fail("runtime_filter may appear only as a predicate root");
            FilterIdentity algorithm = identity(value.get("algorithm"), "runtime_filter.algorithm");
            if (!algorithm.equals(new FilterIdentity("duckdb.runtime_filter", "bloom", 1))
                    && !algorithm.equals(new FilterIdentity("duckdb.runtime_filter", "prefix_range", 1))) {
                fail("unknown runtime-filter algorithm " + algorithm);
            }
            Payload artifact = payload("artifact", value.get("artifact_ref"));
            String nullHandling = text(value.get("null_handling"), "runtime_filter.null_handling", true);
            if (!nullHandling.equals("pass") && !nullHandling.equals("reject")) {
                fail("runtime_filter.null_handling must be 'pass' or 'reject'");
            }
            boolean supported = capabilities.runtimeAlgorithms().contains(algorithm);
            if (supported) fail("runtime-filter algorithm has no registered evaluator");
            return new FilterExpression.RuntimeFilter(algorithm,
                    expression(object(value.get("input"), "runtime_filter.input"), depth + 1, false),
                    artifact.ref(), artifact.field(), artifact.value(), nullHandling.equals("pass"), false);
        }

        private FilterIdentity identity(JsonNode value, String where) {
            JsonNode object = object(value, where);
            keys(object, Set.of("namespace", "name", "version"), Set.of(), where);
            String namespace = text(object.get("namespace"), where + ".namespace", true);
            String name = text(object.get("name"), where + ".name", true);
            if (!NAMESPACE.matcher(namespace).matches() || !NAME.matcher(name).matches()) {
                fail(where + " has a noncanonical identity");
            }
            long version = uint(object.get("version"), where + ".version");
            if (version == 0) fail(where + ".version must be positive");
            return new FilterIdentity(namespace, name, version);
        }

        private record Payload(long ref, Field field, Object value) {}

        private Payload payload(String prefix, JsonNode value) {
            long ref = uint(value, prefix + "_ref");
            String name = prefix + "_" + ref;
            List<Field> fields = root.getSchema().getFields();
            int found = -1;
            for (int i = 0; i < fields.size(); i++) {
                if (!fields.get(i).getName().equals(name)) continue;
                if (found >= 0) fail("duplicate payload field " + name);
                found = i;
            }
            if (found < 0) fail("missing payload field " + name);
            return new Payload(ref, fields.get(found), VectorScalarCodec.read(root.getVector(found), 0));
        }

        private static Field expressionField(FilterExpression expression) {
            return switch (expression) {
                case FilterExpression.ColumnRef value -> value.field();
                case FilterExpression.FieldRef value -> value.field();
                case FilterExpression.Literal value -> value.field();
                case FilterExpression.Cast value -> value.field();
                case FilterExpression.Arithmetic value -> firstKnownField(value.left(), value.right());
                case FilterExpression.Negate value -> expressionField(value.expression());
                case FilterExpression.Comparison ignored -> BOOLEAN_FIELD;
                case FilterExpression.BooleanExpression ignored -> BOOLEAN_FIELD;
                case FilterExpression.Not ignored -> BOOLEAN_FIELD;
                case FilterExpression.IsNull ignored -> BOOLEAN_FIELD;
                case FilterExpression.In ignored -> BOOLEAN_FIELD;
                case FilterExpression.Call ignored -> BOOLEAN_FIELD;
                case FilterExpression.RuntimeFilter ignored -> BOOLEAN_FIELD;
                default -> null;
            };
        }

        private static boolean isBoolean(FilterExpression expression) {
            Field field = expressionField(expression);
            return field != null && field.getType() instanceof ArrowType.Bool;
        }

        private static Field firstKnownField(FilterExpression left, FilterExpression right) {
            Field field = expressionField(left);
            return field == null ? expressionField(right) : field;
        }

        private static void requireNumeric(FilterExpression expression, String where) {
            Field field = expressionField(expression);
            if (field != null && !isNumeric(field.getType())) fail(where + " must resolve to a numeric type");
        }

        private static void validateComparable(FilterExpression left, FilterExpression right, String where) {
            Field leftField = expressionField(left);
            Field rightField = expressionField(right);
            if (leftField != null && rightField != null && !bindCompatible(leftField, rightField)) {
                fail(where + " are not bind-compatible");
            }
        }

        private static void validateMembership(FilterExpression input, FilterExpression.ValueSet set) {
            Field inputField = expressionField(input);
            Field valueField;
            if (set instanceof FilterExpression.LiteralSet literal) {
                valueField = literal.field().getChildren().isEmpty()
                        ? null : literal.field().getChildren().getFirst();
            } else {
                valueField = ((FilterExpression.ExternalSet) set).field();
            }
            if (inputField != null && valueField != null && !bindCompatible(inputField, valueField)) {
                fail("IN input and set element are not bind-compatible");
            }
        }

        private static void validateCall(Object function, List<FilterExpression> arguments) {
            if (function instanceof FilterIdentity) {
                if (arguments.size() != 2) fail("intersects_extent requires two arguments");
                return;
            }
            FilterExpression.StandardFunction standard = (FilterExpression.StandardFunction) function;
            if (arguments.size() != 2) fail(standard.name().toLowerCase() + " requires two arguments");
            Field first = expressionField(arguments.get(0));
            Field second = expressionField(arguments.get(1));
            if (standard == FilterExpression.StandardFunction.LIST_CONTAINS) {
                if (first != null && !(first.getType() instanceof ArrowType.List
                        || first.getType() instanceof ArrowType.LargeList)) {
                    fail("list_contains input must resolve to a list type");
                }
                Field child = first == null || first.getChildren().isEmpty()
                        ? null : first.getChildren().getFirst();
                if (child != null && second != null && !bindCompatible(child, second)) {
                    fail("list_contains arguments are not bind-compatible");
                }
                return;
            }
            if ((first != null && !isString(first.getType()))
                    || (second != null && !isString(second.getType()))) {
                fail(standard.name().toLowerCase() + " arguments must resolve to VARCHAR");
            }
        }

        private static boolean bindCompatible(Field left, Field right) {
            String leftExtension = extensionName(left);
            String rightExtension = extensionName(right);
            if (leftExtension != null || rightExtension != null) {
                return java.util.Objects.equals(leftExtension, rightExtension)
                        && left.getType().equals(right.getType());
            }
            ArrowType leftType = left.getType();
            ArrowType rightType = right.getType();
            if (leftType instanceof ArrowType.Null || rightType instanceof ArrowType.Null) return true;
            if (leftType.equals(rightType)) return true;
            if (isNumeric(leftType) && isNumeric(rightType)) return true;
            if (isString(leftType) && isString(rightType)) return true;
            if (isBinary(leftType) && isBinary(rightType)) return true;
            return isTemporal(leftType) && isTemporal(rightType);
        }

        private static boolean isNumeric(ArrowType type) {
            return type instanceof ArrowType.Int || type instanceof ArrowType.FloatingPoint
                    || type instanceof ArrowType.Decimal;
        }

        private static boolean isString(ArrowType type) {
            return type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8;
        }

        private static boolean isBinary(ArrowType type) {
            return type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary
                    || type instanceof ArrowType.FixedSizeBinary;
        }

        private static boolean isTemporal(ArrowType type) {
            return type instanceof ArrowType.Date || type instanceof ArrowType.Time
                    || type instanceof ArrowType.Timestamp || type instanceof ArrowType.Duration;
        }

        private static boolean requiresContext(FilterExpression expression) {
            return switch (expression) {
                case FilterExpression.Arithmetic value ->
                        value.op() == FilterExpression.ArithmeticOperator.DIVIDE
                                || value.op() == FilterExpression.ArithmeticOperator.MODULO
                                || requiresContext(value.left()) || requiresContext(value.right());
                case FilterExpression.Cast value -> contextual(value.expression(), value.field())
                        || requiresContext(value.expression());
                case FilterExpression.FieldRef value -> requiresContext(value.expression());
                case FilterExpression.Comparison value ->
                        requiresContext(value.left()) || requiresContext(value.right());
                case FilterExpression.BooleanExpression value ->
                        value.children().stream().anyMatch(Parser::requiresContext);
                case FilterExpression.In value -> requiresContext(value.expression());
                case FilterExpression.Not value -> requiresContext(value.expression());
                case FilterExpression.IsNull value -> requiresContext(value.expression());
                case FilterExpression.Negate value -> requiresContext(value.expression());
                case FilterExpression.Call value -> value.arguments().stream().anyMatch(Parser::requiresContext);
                case FilterExpression.RuntimeFilter value -> requiresContext(value.input());
                default -> false;
            };
        }

        private static boolean contextual(FilterExpression source, Field target) {
            Field sourceField = expressionField(source);
            return sourceField != null && contextualType(sourceField.getType()) && contextualType(target.getType());
        }

        private static boolean contextualType(ArrowType type) {
            return type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8
                    || type instanceof ArrowType.Date || type instanceof ArrowType.Time
                    || type instanceof ArrowType.Timestamp;
        }

        private static void validateExtensions(Field field) {
            Set<String> extensions = new HashSet<>();
            String metadataExtension = field.getMetadata() == null ? null
                    : field.getMetadata().get("ARROW:extension:name");
            if (metadataExtension != null) extensions.add(metadataExtension);
            if (field.getType() instanceof ArrowType.ExtensionType extensionType) {
                extensions.add(extensionType.extensionName());
            }
            for (String extension : extensions) {
                if (!KNOWN_EXTENSIONS.contains(extension)) {
                    fail("unknown Arrow extension type " + extension);
                }
            }
            for (Field child : field.getChildren()) validateExtensions(child);
        }

        private static String extensionName(Field field) {
            String metadataExtension = field.getMetadata() == null ? null
                    : field.getMetadata().get("ARROW:extension:name");
            if (metadataExtension != null) return metadataExtension;
            return field.getType() instanceof ArrowType.ExtensionType extensionType
                    ? extensionType.extensionName() : null;
        }

        private static List<String> contextKeys() {
            return List.of("vgi_time_zone", "vgi_calendar", "vgi_default_collation",
                    "vgi_ieee_floating_point_ops", "vgi_integer_division",
                    "vgi_context_provider_fingerprint");
        }

        private static Boolean contextBoolean(Map<String, String> metadata, String key) {
            String value = metadata.get(key);
            if (!"true".equals(value) && !"false".equals(value)) fail(key + " must be canonical true or false");
            return Boolean.valueOf(value);
        }

        private static JsonNode required(JsonNode object, String name, String where) {
            if (object == null || !object.has(name)) fail(where + " is missing " + name);
            return object.get(name);
        }

        private static JsonNode object(JsonNode value, String where) {
            if (value == null || !value.isObject()) fail(where + " must be an object");
            return value;
        }

        private static List<JsonNode> array(JsonNode value, String where) {
            if (value == null || !value.isArray()) fail(where + " must be an array");
            List<JsonNode> result = new ArrayList<>();
            value.forEach(result::add);
            return result;
        }

        private static String text(JsonNode value, String where, boolean nonempty) {
            if (value == null || !value.isTextual() || (nonempty && value.textValue().isEmpty())) {
                fail(where + " must be " + (nonempty ? "a nonempty " : "an ") + "UTF-8 string");
            }
            return value.textValue();
        }

        private static boolean bool(JsonNode value, String where) {
            if (value == null || !value.isBoolean()) fail(where + " must be a Boolean");
            return value.booleanValue();
        }

        private static long uint(JsonNode value, String where) {
            if (value == null || !value.isIntegralNumber()) {
                fail(where + " must be an unsigned 64-bit integer");
            }
            BigInteger result = value.bigIntegerValue();
            if (result.signum() < 0 || result.compareTo(UINT64_MAX) > 0) {
                fail(where + " must be an unsigned 64-bit integer");
            }
            return result.longValue();
        }

        private static boolean outOfRange(long value, int size) {
            return value < 0 || value >= size;
        }

        private static String id(JsonNode value, String where) {
            String result = text(value, where + ".id", true);
            if (result.getBytes(StandardCharsets.UTF_8).length > MAX_ID_BYTES) {
                fail(where + ".id exceeds 128 UTF-8 bytes");
            }
            return result;
        }

        private static void keys(JsonNode object, Set<String> required, Set<String> optional, String where) {
            Set<String> present = new HashSet<>();
            object.fieldNames().forEachRemaining(present::add);
            Set<String> missing = new HashSet<>(required);
            missing.removeAll(present);
            if (!missing.isEmpty()) fail(where + " is missing " + missing);
            present.removeAll(required);
            present.removeAll(optional);
            if (!present.isEmpty()) fail(where + " has unknown properties " + present);
        }

        private static void fail(String message) {
            throw new FilterV2Exception(message);
        }
    }
}
