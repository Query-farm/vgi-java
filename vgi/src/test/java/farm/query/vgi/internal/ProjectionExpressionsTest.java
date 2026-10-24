// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;

import static org.junit.jupiter.api.Assertions.*;

import farm.query.vgi.protocol.*;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.marshal.RecordCodec;
import java.util.*;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.Test;

class ProjectionExpressionsTest {
    private static final QueryEvaluationContext CONTEXT =
        new QueryEvaluationContext("UTC", "binary", "asc", "nulls_last", true);
    private static final Schema SOURCE =
        new Schema(List.of(Schemas.nullable("n", Schemas.INT32)), Map.of("source", "retained"));
    private static final Field RESULT =
        new Field("wide", new FieldType(true, Schemas.INT64, null, Map.of("requested", "retained")), null);
    private static TableFunctionProjectionRequest proposal(List<String> owner) {
        var call = new BindRequest("projection_sequence", new byte[0], "table", null, null, null, null, null,
                                   false, null, null, null, null, owner);
        var bind = new BindResponse(SchemaUtil.serializeSchema(SOURCE), new byte[0], List.of(), List.of(),
                                    List.of(), List.of(ProjectionExpressions.PROFILE));
        var expression =
            new SqlExpression("CAST(\"n\" AS BIGINT)", ProjectionExpressions.PROFILE, owner, CONTEXT);
        return new TableFunctionProjectionRequest(RecordCodec.serializeToBytes(call),
                                                  RecordCodec.serializeToBytes(bind),
                                                  List.of(RecordCodec.serializeToBytes(expression)),
                                                  SchemaUtil.serializeSchema(new Schema(List.of(RESULT))));
    }
    private static TableFunctionProjectionResponse accepted(Schema schema) {
        var bind = new BindResponse(SchemaUtil.serializeSchema(schema), new byte[] {42}, List.of(), List.of(),
                                    List.of(), List.of(ProjectionExpressions.PROFILE));
        return new TableFunctionProjectionResponse(List.of(0L), RecordCodec.serializeToBytes(bind));
    }
    @Test
    void rejectsNullDictionaryPromiseWithRetainedNameBytes() throws Exception {
        var request = proposal(List.of("main"));
        byte[] malformed;
        try (var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                 new java.io.ByteArrayInputStream(request.bind_result()), farm.query.vgirpc.wire.Allocators.root())) {
            assertTrue(reader.loadNextBatch());
            var row = reader.getVectorSchemaRoot();
            var field = row.getSchema().findField("repeatability");
            var values = reader.lookup(field.getDictionary().getId()).getVector();
            // Preserve UNKNOWN's offsets and bytes while nulling only its validity bit.
            values.getValidityBuffer().setByte(0, 0);
            var sink = new java.io.ByteArrayOutputStream();
            try (var writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(row, reader, sink)) {
                writer.start(); writer.writeBatch(); writer.end();
            }
            malformed = sink.toByteArray();
        }
        var invalid = new TableFunctionProjectionRequest(request.bind_call(), malformed,
            request.expressions(), request.result_schema());
        assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.validateRequest(invalid));
    }
    @Test
    void acceptsExactAppendedFieldsAndPortableState() {
        var request = proposal(List.of("main"));
        var fields = new ArrayList<>(SOURCE.getFields());
        fields.add(RESULT);
        var response = accepted(new Schema(fields, SOURCE.getCustomMetadata()));
        ProjectionExpressions.validateResponse(request, response);
        var bind = RecordCodec.deserializeFromBytes(response.bind_result(), BindResponse.class);
        var reopened = ProjectionExpressions.open(ProjectionExpressions.envelope(bind, RecordCodec.deserializeFromBytes(request.bind_call(), BindRequest.class)), RecordCodec.deserializeFromBytes(request.bind_call(), BindRequest.class));
        assertArrayEquals(bind.output_schema(), reopened.output_schema());
        assertArrayEquals(new byte[] {42}, reopened.opaque_data());
        ProjectionExpressions.validateResponse(request, TableFunctionProjectionResponse.decline());
    }
    @Test
    void rejectsMalformedAcceptanceAndEnvelope() {
        var request = proposal(List.of("main"));
        assertThrows(IllegalArgumentException.class,
                     () -> ProjectionExpressions.validateResponse(request, accepted(SOURCE)));
        assertThrows(
            IllegalArgumentException.class,
            ()
                -> ProjectionExpressions.validateResponse(
                    request, new TableFunctionProjectionResponse(List.of(-1L), request.bind_result())));
        assertThrows(
            IllegalArgumentException.class,
            ()
                -> ProjectionExpressions.validateResponse(
                    request, new TableFunctionProjectionResponse(List.of(0L, 0L), request.bind_result())));
        assertThrows(IllegalArgumentException.class,
                     ()
                         -> ProjectionExpressions.validateResponse(
                             request, new TableFunctionProjectionResponse(List.of(), request.bind_result())));
        var trailing = Arrays.copyOf(request.expressions().get(0), request.expressions().get(0).length + 1);
        var malformed = new TableFunctionProjectionRequest(request.bind_call(), request.bind_result(),
                                                           List.of(trailing), request.result_schema());
        assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.validateRequest(malformed));
    }
    @Test
    void validatesOffersAndSupportsExplicitRootOwner() {
        ProjectionExpressions.validateOffer(List.of(ProjectionExpressions.PROFILE, "provider.future.v2"));
        assertThrows(IllegalArgumentException.class,
                     () -> ProjectionExpressions.validateOffer(List.of("vgi.bad")));
        assertThrows(IllegalArgumentException.class,
                     ()
                         -> ProjectionExpressions.validateOffer(
                             List.of(ProjectionExpressions.PROFILE, ProjectionExpressions.PROFILE)));
        ProjectionExpressions.validateRequest(proposal(List.of()));
        assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.size(8L * 1024 * 1024 + 1));
    }
    @Test void includesArrowFramingInSizeLimitBeforeNestedDecode() {
        var request = proposal(List.of("main"));
        int other = request.bind_call().length + request.bind_result().length + request.expressions().get(0).length;
        var oversized = new TableFunctionProjectionRequest(request.bind_call(), request.bind_result(), request.expressions(),
            new byte[8 * 1024 * 1024 - other - 16]);
        var error = assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.validateRequest(oversized));
        assertTrue(error.getMessage().contains("exceeds 8 MiB"));
    }

    private static final class Offered extends farm.query.vgi.table.SimpleTableFunction {
        private final String name;
        private final BindRepeatability repeatability;
        private final List<String> profiles;
        Offered(String name) { this(name, BindRepeatability.UNKNOWN, List.of(ProjectionExpressions.PROFILE)); }
        Offered(String name, BindRepeatability repeatability, List<String> profiles) {
            this.name = name; this.repeatability = repeatability; this.profiles = profiles;
        }
        @Override public farm.query.vgi.function.FunctionSpec spec() {
            return farm.query.vgi.function.FunctionSpec.builder(name).description("portable projection probe").build();
        }
        @Override protected Schema outputSchema() { return SOURCE; }
        @Override public BindResponse onBind(farm.query.vgi.table.TableBindParams params) {
            return new BindResponse(SchemaUtil.serializeSchema(SOURCE), new byte[]{42}, List.of(), List.of(), List.of(), profiles, repeatability);
        }
        @Override public TableFunctionProjectionResponse projection(TableFunctionProjectionRequest request) {
            assertArrayEquals(new byte[]{42}, RecordCodec.deserializeFromBytes(request.bind_result(), BindResponse.class).opaque_data());
            var fields = new ArrayList<>(SOURCE.getFields()); fields.add(RESULT);
            return accepted(new Schema(fields, SOURCE.getCustomMetadata()));
        }
        @Override public farm.query.vgi.table.TableProducerState createProducer(farm.query.vgi.table.TableInitParams params) {
            assertArrayEquals(new byte[]{42}, params.bindOpaqueData());
            return new farm.query.vgi.table.TableProducerState() {
                @Override public void produceTick(farm.query.vgirpc.OutputCollector out, farm.query.vgirpc.CallContext ctx) { out.finish(); }
            };
        }
    }
    private static VgiServiceImpl portableService() {
        var worker = farm.query.vgi.Worker.builder().catalogName("probe").registerTable(new Offered("projection_sequence"));
        return new VgiServiceImpl(worker, worker.scalars(), worker.tables(), worker.tableInOuts(), worker.aggregates());
    }
    private static InitRequest init(BindRequest call, BindResponse bind) {
        return new InitRequest(RecordCodec.serializeToBytes(call), bind.output_schema(), bind.opaque_data(),
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }
    @Test void offeredOriginalStateIsStableAndRehydratesWithoutPendingCache() {
        var template = proposal(List.of("main"));
        var call = RecordCodec.deserializeFromBytes(template.bind_call(), BindRequest.class);
        var service = portableService();
        var first = service.bind(call, null);
        var second = service.bind(call, null);
        assertArrayEquals(RecordCodec.serializeToBytes(first), RecordCodec.serializeToBytes(second));
        assertArrayEquals(RecordCodec.serializeToBytes(first), RecordCodec.serializeToBytes(portableService().bind(call, null)));
        assertDoesNotThrow(() -> portableService().init(init(call, first), null));
        var request = new TableFunctionProjectionRequest(template.bind_call(), RecordCodec.serializeToBytes(first), template.expressions(), template.result_schema());
        var response = RecordCodec.deserializeFromBytes(portableService().table_function_projection(RecordCodec.serializeToBytes(request), null), TableFunctionProjectionResponse.class);
        var accepted = RecordCodec.deserializeFromBytes(response.bind_result(), BindResponse.class);
        assertDoesNotThrow(() -> portableService().init(init(call, accepted), null));
        assertDoesNotThrow(() -> portableService().init(init(call, first), null));
        var different = new BindRequest("other_function", call.arguments(), call.function_type(), null, null, null, null, null, false, null, null, null, null, List.of("main"));
        assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.open(first.opaque_data(), different));
    }

    @Test void validatesRawOuterEnvelopeBeforeTypedDecoding() throws Exception {
        byte[] raw = RecordCodec.serializeToBytes(proposal(List.of("main")));
        assertEquals(1, ProjectionExpressions.decodeRequest(raw).expressions().size());
        assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.decodeRequest(Arrays.copyOf(raw, raw.length + 1)));
        try (var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(new java.io.ByteArrayInputStream(raw), farm.query.vgirpc.wire.Allocators.root());
             var sink = new java.io.ByteArrayOutputStream()) {
            assertTrue(reader.loadNextBatch());
            try (var writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(reader.getVectorSchemaRoot(), null, sink)) {
                writer.start(); writer.writeBatch(); writer.writeBatch(); writer.end();
            }
            assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.decodeRequest(sink.toByteArray()));
        }
    }

    @Test void repeatabilityDefaultsAndProjectionPreservation() {
        assertEquals(BindRepeatability.UNKNOWN, BindResponse.forSchema(SchemaUtil.serializeSchema(SOURCE)).repeatability());
        assertThrows(IllegalArgumentException.class, () -> new BindResponse(SchemaUtil.serializeSchema(SOURCE), null,
                List.of(), List.of(), List.of(), List.of(), null));
        var template = proposal(List.of("main"));
        var original = RecordCodec.deserializeFromBytes(template.bind_result(), BindResponse.class);
        var fields = new ArrayList<>(SOURCE.getFields()); fields.add(RESULT);
        for (var promise : BindRepeatability.values()) {
            var bound = new BindResponse(original.output_schema(), original.opaque_data(), List.of(), List.of(), List.of(), original.projection_expression_profiles(), promise);
            var request = new TableFunctionProjectionRequest(template.bind_call(), RecordCodec.serializeToBytes(bound), template.expressions(), template.result_schema());
            var good = new BindResponse(SchemaUtil.serializeSchema(new Schema(fields, SOURCE.getCustomMetadata())), new byte[]{42}, List.of(), List.of(), List.of(), original.projection_expression_profiles(), promise);
            ProjectionExpressions.validateResponse(request, new TableFunctionProjectionResponse(List.of(0L), RecordCodec.serializeToBytes(good)));
            var other = promise == BindRepeatability.UNKNOWN ? BindRepeatability.STABLE_WITHIN_QUERY : BindRepeatability.UNKNOWN;
            var bad = new BindResponse(good.output_schema(), good.opaque_data(), List.of(), List.of(), List.of(), good.projection_expression_profiles(), other);
            assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.validateResponse(request, new TableFunctionProjectionResponse(List.of(0L), RecordCodec.serializeToBytes(bad))));
        }
    }
    @Test void stableUnofferedBindUsesPortableOriginalState() {
        var worker = farm.query.vgi.Worker.builder().catalogName("probe").registerTable(
                new Offered("projection_sequence", BindRepeatability.STABLE_WITHIN_QUERY, List.of()));
        var service = new VgiServiceImpl(worker, worker.scalars(), worker.tables(), worker.tableInOuts(), worker.aggregates());
        var call = RecordCodec.deserializeFromBytes(proposal(List.of("main")).bind_call(), BindRequest.class);
        var first = service.bind(call, null);
        assertEquals(BindRepeatability.STABLE_WITHIN_QUERY, first.repeatability());
        assertTrue(first.projection_expression_profiles().isEmpty());
        assertArrayEquals(RecordCodec.serializeToBytes(first), RecordCodec.serializeToBytes(service.bind(call, null)));
        var fresh = new VgiServiceImpl(worker, worker.scalars(), worker.tables(), worker.tableInOuts(), worker.aggregates());
        assertDoesNotThrow(() -> fresh.init(init(call, first), null));
    }

}
