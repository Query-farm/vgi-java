// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;

import farm.query.vgi.protocol.QueryEvaluationContext;
import farm.query.vgi.protocol.SqlExpression;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.wire.Allocators;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Canonical VIRTUAL expression carriers. Validation never evaluates SQL. */
public final class ColumnExpressions {
    public static final String GENERATED = "vgi.column.generated.v1";
    public static final String GENERATION = "vgi.column.generation.v1";
    private static final String PROFILE = "vgi.expression.duckdb.scalar.v1";
    private static final int MAXIMUM = 8 * 1024 * 1024;
    private ColumnExpressions() {}
    private static void text(String value, int maximum) throws Exception {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
            || StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(value))
                    .remaining()
                > maximum)
            throw new IllegalArgumentException("Invalid expression text");
    }
    private static void expression(SqlExpression value, List<String> owner) throws Exception {
        text(value.sql(), 1024 * 1024);
        if (value.sql().isBlank() || !PROFILE.equals(value.profile()))
            throw new IllegalArgumentException("Unsupported generated profile or empty SQL");
        if (value.schema_path() == null || value.schema_path().size() > 32)
            throw new IllegalArgumentException("Invalid expression owner");
        for (var part : value.schema_path()) text(part, 4096);
        if (!value.schema_path().equals(owner))
            throw new IllegalArgumentException("Expression owner differs from table schema");
        var context = value.evaluation_context();
        if (context == null)
            throw new IllegalArgumentException("Missing expression context");
        text(context.timezone(), 256);
        text(context.collation(), 256);
        if (!List.of("asc", "desc").contains(context.default_order())
            || !List.of("nulls_first", "nulls_last", "nulls_first_on_asc_last_on_desc",
                        "nulls_last_on_asc_first_on_desc")
                    .contains(context.default_null_order()))
            throw new IllegalArgumentException("Invalid expression evaluation context");
    }
    private static boolean sameFields(List<Field> actual, List<Field> expected) {
        if (actual.size() != expected.size())
            return false;
        for (int i = 0; i < actual.size(); i++) {
            var a = actual.get(i);
            var e = expected.get(i);
            if (!a.getName().equals(e.getName()) || a.isNullable() != e.isNullable()
                || !a.getType().equals(e.getType())
                || !sameFields(a.getChildren(), e.getChildren()))
                return false;
        }
        return true;
    }
    private static void required(FieldVector vector) throws Exception {
        if (vector.getNullCount() != 0)
            throw new IllegalArgumentException("Required expression value is NULL");
        if (vector instanceof VarCharVector strings)
            for (int i = 0; i < strings.getValueCount(); i++)
                StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(strings.get(i)));
        for (var child : vector.getChildrenFromFields()) required(child);
    }
    // Bound each IPC message before Arrow's reader allocates its metadata buffer.
    private static void envelope(byte[] bytes) {
        var data = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int offset = 0, messages = 0;
        while (offset < bytes.length) {
            int prefix = data.getInt(offset) == -1 ? 8 : 4;
            int length = data.getInt(offset + prefix - 4);
            offset += prefix;
            if (length == 0) {
                if (offset != bytes.length || messages != 2) throw new IllegalArgumentException("Expression requires one complete IPC batch");
                return;
            }
            if (length < 0 || length > bytes.length - offset) throw new IllegalArgumentException("Invalid expression IPC length");
            var message = org.apache.arrow.flatbuf.Message.getRootAsMessage(ByteBuffer.wrap(bytes, offset, length).slice().order(ByteOrder.LITTLE_ENDIAN));
            int expected = messages == 0 ? org.apache.arrow.flatbuf.MessageHeader.Schema : org.apache.arrow.flatbuf.MessageHeader.RecordBatch;
            if (messages >= 2 || message.headerType() != expected) throw new IllegalArgumentException("Invalid expression IPC message");
            long body = message.bodyLength();
            if (body < 0 || body > bytes.length - offset - length) throw new IllegalArgumentException("Invalid expression IPC body");
            offset += length + (int) body;
            messages++;
        }
        throw new IllegalArgumentException("Expression IPC lacks end marker");
    }
    public static void encoded(String encoded, List<String> owner) throws Exception {
        if (encoded.isEmpty() || encoded.length() > 4 * ((MAXIMUM + 2) / 3))
            throw new IllegalArgumentException("Invalid expression size");
        byte[] bytes = Base64.getDecoder().decode(encoded);
        if (bytes.length == 0 || bytes.length > MAXIMUM
            || !Base64.getEncoder().encodeToString(bytes).equals(encoded))
            throw new IllegalArgumentException("Expression requires canonical base64 IPC");
        envelope(bytes);
        var stream = new ByteArrayInputStream(bytes);
        try (var reader = new ArrowStreamReader(stream, Allocators.root())) {
            if (!reader.loadNextBatch())
                throw new IllegalArgumentException("Missing expression row");
            var row = reader.getVectorSchemaRoot();
            if (row.getRowCount() != 1
                || !sameFields(row.getSchema().getFields(),
                    SchemaDerivation.schemaForRecord(SqlExpression.class).getFields()))
                throw new IllegalArgumentException("Invalid SqlExpression record schema");
            for (var vector : row.getFieldVectors()) required(vector);
            if (reader.loadNextBatch() || stream.available() != 0)
                throw new IllegalArgumentException(
                    "Expression requires exactly one complete IPC batch");
        }
        expression(RecordCodec.deserializeFromBytes(bytes, SqlExpression.class), owner);
    }
    public static String generatedMetadata(String sql, List<String> owner) {
        try {
            var value = new SqlExpression(sql, PROFILE, List.copyOf(owner),
                new QueryEvaluationContext("UTC", "binary", "asc", "nulls_last", true));
            expression(value, owner);
            return Base64.getEncoder().encodeToString(RecordCodec.serializeToBytes(value));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid generated expression", e);
        }
    }
    public static void validate(Schema schema, List<String> owner, boolean allowGenerated) {
        try {
            for (var field : schema.getFields()) field(field, owner, allowGenerated);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid generated column metadata", e);
        }
    }
    private static void field(Field field, List<String> owner, boolean allowGenerated)
        throws Exception {
        Map<String, String> values = field.getMetadata();
        for (var key : values.keySet())
            if ((key.startsWith("vgi.column.generated.") && !key.equals(GENERATED))
                || (key.startsWith("vgi.column.generation.") && !key.equals(GENERATION)))
                throw new IllegalArgumentException("Unknown generated metadata version");
        if (values.containsKey("generated_expression"))
            throw new IllegalArgumentException(
                "Legacy generated expression requires explicit canonical translation");
        if (values.containsKey(GENERATED) != values.containsKey(GENERATION))
            throw new IllegalArgumentException(
                "Generated expression and category must occur together");
        if (values.containsKey(GENERATED)) {
            if (!"virtual".equals(values.get(GENERATION)))
                throw new IllegalArgumentException("Only VIRTUAL generated columns are supported");
            if (values.containsKey("default") || values.containsKey("vgi.column.default.v1"))
                throw new IllegalArgumentException("Generated column cannot have a default");
            encoded(values.get(GENERATED), owner);
            if (!allowGenerated)
                throw new UnsupportedOperationException(
                    "Generated columns unsupported by this catalog");
        }
        for (var child : field.getChildren()) field(child, owner, allowGenerated);
    }
    public static void validateIpc(byte[] bytes, List<String> owner, boolean allowGenerated) {
        try (var reader =
                 new ArrowStreamReader(new ByteArrayInputStream(bytes), Allocators.root())) {
            validate(reader.getVectorSchemaRoot().getSchema(), owner, allowGenerated);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid generated column schema", e);
        }
    }
}
