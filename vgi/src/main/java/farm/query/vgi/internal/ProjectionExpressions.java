// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;
import farm.query.vgi.protocol.*;
import farm.query.vgirpc.marshal.RecordCodec;
import java.util.*;
import org.apache.arrow.vector.types.pojo.Schema;
/** Strict carrier/append-only validation; no SQL evaluation. */
public final class ProjectionExpressions {
    private ProjectionExpressions() {}
    public static final String PROFILE = "vgi.expression.duckdb.scalar.v1";
    private static final byte[] MARKER =
        "vgi.projection.v1\0".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    public static void validateOffer(List<String> profiles) {
        if (profiles == null || profiles.size() > 64 || new HashSet<>(profiles).size() != profiles.size() ||
            profiles.stream().anyMatch(
                p
                -> p == null || p.length() > 256 ||
                       !p.matches("[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)*\\.v[1-9][0-9]*")))
            throw new IllegalArgumentException("Invalid projection profiles");
    }
    private static String identifier(String value) {
        var result = new StringBuilder(value.length());
        for (char c : value.toCharArray())
            result.append(c >= 'A' && c <= 'Z' ? (char)(c + 32) : c);
        return result.toString();
    }
    public static void size(long length) {
        if (length > 8 * 1024 * 1024)
            throw new IllegalArgumentException("Projection payload exceeds 8 MiB");
    }
    /** Preserve and validate the outer binary carrier before record decoding can discard rows. */
    public static TableFunctionProjectionRequest decodeRequest(byte[] bytes) {
        size(bytes.length);
        var buffer = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int offset = 0, messages = 0;
        boolean ended = false;
        while (offset < bytes.length) {
            if (bytes.length - offset < 4) throw new IllegalArgumentException("Truncated projection IPC prefix");
            int prefix = buffer.getInt(offset) == -1 ? 8 : 4;
            if (bytes.length - offset < prefix) throw new IllegalArgumentException("Truncated projection IPC prefix");
            int length = buffer.getInt(offset + prefix - 4);
            offset += prefix;
            if (length == 0) { ended = offset == bytes.length && messages == 2; break; }
            if (length < 0 || length > bytes.length - offset) throw new IllegalArgumentException("Invalid projection IPC length");
            var message = org.apache.arrow.flatbuf.Message.getRootAsMessage(
                    java.nio.ByteBuffer.wrap(bytes, offset, length).slice().order(java.nio.ByteOrder.LITTLE_ENDIAN));
            int expected = messages == 0 ? org.apache.arrow.flatbuf.MessageHeader.Schema : org.apache.arrow.flatbuf.MessageHeader.RecordBatch;
            if (messages >= 2 || message.headerType() != expected) throw new IllegalArgumentException("Projection requires exactly one batch");
            long body = message.bodyLength();
            if (body < 0 || body > bytes.length - offset - length) throw new IllegalArgumentException("Invalid projection IPC body");
            offset += length + (int)body;
            messages++;
        }
        if (!ended) throw new IllegalArgumentException("Projection requires one complete IPC batch");
        try (var source = new java.io.ByteArrayInputStream(bytes);
             var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(source, farm.query.vgirpc.wire.Allocators.root())) {
            if (!reader.loadNextBatch()) throw new IllegalArgumentException("Projection record missing");
            var row = reader.getVectorSchemaRoot();
            var expected = farm.query.vgirpc.schema.SchemaDerivation.schemaForRecord(TableFunctionProjectionRequest.class);
            if (row.getRowCount() != 1 || !row.getSchema().equals(expected))
                throw new IllegalArgumentException("Projection requires one row with its exact schema");
            for (var vector : row.getFieldVectors())
                if (vector.getNullCount() != 0) throw new IllegalArgumentException("Required projection field is NULL");
            if (reader.loadNextBatch() || source.available() != 0)
                throw new IllegalArgumentException("Projection requires one complete IPC batch");
        } catch (java.io.IOException error) {
            throw new IllegalArgumentException("Invalid projection IPC", error);
        }
        return RecordCodec.deserializeFromBytes(bytes, TableFunctionProjectionRequest.class);
    }
    public static BindRequest validateRequest(TableFunctionProjectionRequest request) {
        size((long)request.bind_call().length + request.bind_result().length +
             request.result_schema().length + request.expressions().stream().mapToLong(b -> b.length).sum());
        size(RecordCodec.serializeToBytes(request).length);
        var bind = RecordCodec.deserializeFromBytes(request.bind_call(), BindRequest.class);
        var source = RecordCodec.deserializeFromBytes(request.bind_result(), BindResponse.class);
        validateOffer(source.projection_expression_profiles());
        if (!"table".equalsIgnoreCase(bind.function_type()) || bind.input_schema() != null ||
            bind.schema_path() == null)
            throw new IllegalArgumentException("Projection requires ordinary table and explicit owner");
        if (!source.lookup_names().isEmpty() || !source.lookup_scopes().isEmpty() ||
            !source.lookup_secret_types().isEmpty())
            throw new IllegalArgumentException("Projection cannot request secrets");
        var schema = SchemaUtil.deserializeSchema(source.output_schema());
        var result = SchemaUtil.deserializeSchema(request.result_schema());
        if (request.expressions().isEmpty() || request.expressions().size() > 1024 ||
            result.getFields().size() != request.expressions().size() ||
            !result.getCustomMetadata().isEmpty())
            throw new IllegalArgumentException("Invalid projection result schema/count");
        var names = new HashSet<String>();
        for (var field :
             java.util.stream.Stream.concat(schema.getFields().stream(), result.getFields().stream())
                 .toList()) {
            var name = identifier(field.getName());
            if (name.isEmpty() || name.indexOf('\0') >= 0 || !names.add(name))
                throw new IllegalArgumentException("Ambiguous projection column names");
        }
        QueryEvaluationContext context = null;
        for (var raw : request.expressions()) {
            try {
                ColumnExpressions.encoded(Base64.getEncoder().encodeToString(raw), bind.schema_path());
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid projection expression", e);
            }
            var expression = RecordCodec.deserializeFromBytes(raw, SqlExpression.class);
            if (!source.projection_expression_profiles().contains(expression.profile()))
                throw new IllegalArgumentException("Projection profile was not offered");
            if (context != null && !context.equals(expression.evaluation_context()))
                throw new IllegalArgumentException("Projection contexts differ");
            context = expression.evaluation_context();
        }
        return bind;
    }
    public static void validateResponse(TableFunctionProjectionRequest request,
                                        TableFunctionProjectionResponse response) {
        validateRequest(request);
        size((long)response.accepted_indices().size() * 8 +
             (response.bind_result() == null ? 0 : response.bind_result().length));
        size(RecordCodec.serializeToBytes(response).length);
        if (response.accepted_indices().isEmpty() != (response.bind_result() == null))
            throw new IllegalArgumentException("Indices and accepted bind result must occur together");
        long previous = -1;
        for (long i : response.accepted_indices()) {
            if (i <= previous || i >= request.expressions().size())
                throw new IllegalArgumentException("Invalid accepted projection index");
            previous = i;
        }
        if (response.bind_result() == null)
            return;
        var original = RecordCodec.deserializeFromBytes(request.bind_result(), BindResponse.class);
        var accepted = RecordCodec.deserializeFromBytes(response.bind_result(), BindResponse.class);
        validateOffer(accepted.projection_expression_profiles());
        if (!accepted.projection_expression_profiles().equals(original.projection_expression_profiles()) ||
            !accepted.lookup_names().isEmpty() || !accepted.lookup_scopes().isEmpty() ||
            !accepted.lookup_secret_types().isEmpty())
            throw new IllegalArgumentException("Accepted projection changed offer/secrets");
        var source = SchemaUtil.deserializeSchema(original.output_schema());
        var result = SchemaUtil.deserializeSchema(request.result_schema());
        var fields = new ArrayList<>(source.getFields());
        for (long i : response.accepted_indices())
            fields.add(result.getFields().get((int)i));
        if (!new Schema(fields, source.getCustomMetadata())
                 .equals(SchemaUtil.deserializeSchema(accepted.output_schema())))
            throw new IllegalArgumentException("Accepted projection changed append-only schema");
    }
    /** Internal stable state, scoped to the complete original invocation identity. */
    public record BindEnvelope(byte[] bind_call, byte[] bind_result)
            implements farm.query.vgirpc.schema.ArrowSerializableRecord {}

    public static byte[] envelope(BindResponse result, BindRequest call) {
        byte[] raw = RecordCodec.serializeToBytes(new BindEnvelope(
                RecordCodec.serializeToBytes(call), RecordCodec.serializeToBytes(result)));
        byte[] value = Arrays.copyOf(MARKER, MARKER.length + raw.length);
        System.arraycopy(raw, 0, value, MARKER.length, raw.length);
        size(value.length);
        return value;
    }
    public static BindResponse open(byte[] value, BindRequest call) {
        if (value == null || value.length < MARKER.length ||
            !Arrays.equals(MARKER, Arrays.copyOf(value, MARKER.length)))
            return null;
        size(value.length);
        var envelope = RecordCodec.deserializeFromBytes(Arrays.copyOfRange(value, MARKER.length, value.length),
                BindEnvelope.class);
        if (call == null || !Arrays.equals(envelope.bind_call(), RecordCodec.serializeToBytes(call)))
            throw new IllegalArgumentException("Projection bind invocation identity changed");
        return RecordCodec.deserializeFromBytes(envelope.bind_result(), BindResponse.class);
    }
}
