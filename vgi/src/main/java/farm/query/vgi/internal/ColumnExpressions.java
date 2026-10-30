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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
    /** The pre-vgi.v3 metadata key carrying a generated column's raw SQL. */
    public static final String LEGACY_GENERATED = "generated_expression";

    /**
     * Refuses a generated column the extension cannot serve: it must be nullable and of a
     * Boolean, signed-integer (8-64 bit) or UTF-8 string type, not an extension type.
     *
     * @param field the generated column
     */
    static void requireGeneratedShape(Field field) {
        if (!field.isNullable())
            throw new IllegalArgumentException("Column " + field.getName()
                + ": generated columns must be nullable");
        var type = field.getType();
        boolean ok = !(type instanceof ArrowType.ExtensionType)
            && !field.getMetadata().containsKey("ARROW:extension:name")
            && field.getDictionary() == null
            && (type instanceof ArrowType.Bool || type instanceof ArrowType.Utf8
                || (type instanceof ArrowType.Int t && t.getIsSigned()));
        if (!ok)
            throw new IllegalArgumentException("Column " + field.getName()
                + ": generated columns require a BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT or VARCHAR type");
    }

    /**
     * The canonical metadata for a VIRTUAL generated column, checked against the grammar the
     * extension reads.
     *
     * @param sql   the DuckDB scalar expression
     * @param owner the owning table's schema path
     * @return the {@code vgi.column.generated.v1} / {@code vgi.column.generation.v1} entries
     */
    public static Map<String, String> virtualMetadata(String sql, List<String> owner) {
        GeneratedExpressionGrammar.validate(sql);
        return Map.of(GENERATED, generatedMetadata(sql, owner), GENERATION, "virtual");
    }

    /**
     * Translates legacy {@code generated_expression} metadata (raw SQL) on top-level columns
     * into the canonical VIRTUAL form, when the column carries no canonical generated
     * metadata of its own. The translated column is served nullable.
     *
     * @param schema the schema
     * @param owner  the owning table's schema path
     * @return the schema, translated
     */
    public static Schema translateLegacy(Schema schema, List<String> owner) {
        boolean changed = false;
        List<Field> out = new java.util.ArrayList<>(schema.getFields().size());
        for (Field f : schema.getFields()) {
            Map<String, String> md = f.getMetadata();
            if (md == null || !md.containsKey(LEGACY_GENERATED) || md.containsKey(GENERATED)
                    || md.containsKey(GENERATION)) {
                out.add(f);
                continue;
            }
            Map<String, String> merged = new java.util.LinkedHashMap<>(md);
            String sql = merged.remove(LEGACY_GENERATED);
            merged.putAll(virtualMetadata(sql, owner));
            out.add(new Field(f.getName(), new FieldType(true, f.getType(), f.getDictionary(), merged),
                f.getChildren()));
            changed = true;
        }
        return changed ? new Schema(out, schema.getCustomMetadata()) : schema;
    }

    /**
     * {@link #translateLegacy(Schema, List)} applied to a served table.
     *
     * @param info the table
     * @return the table, translated
     */
    public static farm.query.vgi.protocol.TableInfo translateLegacy(farm.query.vgi.protocol.TableInfo info) {
        Schema schema = SchemaUtil.deserializeSchema(info.columns());
        Schema translated = translateLegacy(schema, info.schema_path());
        if (translated == schema) return info;
        return new farm.query.vgi.protocol.TableInfo(info.comment(), info.tags(), info.name(), info.schema_path(),
            SchemaUtil.serializeSchema(translated), info.constraints(), info.definition_revision(),
            info.write_result_modes(), info.supports_column_statistics(), info.scan_function(),
            info.insert_function(), info.update_function(), info.delete_function(), info.cardinality_estimate(),
            info.cardinality_max(), info.column_statistics(), info.bind_result(), info.required_filters(),
            info.options(), info.layout());
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
            throw new IllegalArgumentException("Invalid generated column metadata: " + e.getMessage(), e);
        }
    }
    private static void field(Field field, List<String> owner, boolean allowGenerated)
        throws Exception {
        Map<String, String> values = field.getMetadata();
        for (var key : values.keySet())
            if ((key.startsWith("vgi.column.generated.") && !key.equals(GENERATED))
                || (key.startsWith("vgi.column.generation.") && !key.equals(GENERATION)))
                throw new IllegalArgumentException("Unknown generated metadata version");
        if (values.containsKey(LEGACY_GENERATED))
            throw new IllegalArgumentException("Column " + field.getName()
                + ": legacy generated_expression metadata could not be translated (it is combined with"
                + " canonical generated metadata); declare it with CatalogColumns.virtualGenerated(..)");
        if (values.containsKey(GENERATED) != values.containsKey(GENERATION))
            throw new IllegalArgumentException(
                "Generated expression and category must occur together");
        if (values.containsKey(GENERATED)) {
            if (!"virtual".equals(values.get(GENERATION)))
                throw new IllegalArgumentException("Column " + field.getName()
                    + ": only VIRTUAL generated columns are supported (got " + values.get(GENERATION) + ")");
            if (values.containsKey("default") || values.containsKey("vgi.column.default.v1"))
                throw new IllegalArgumentException("Generated column cannot have a default");
            encoded(values.get(GENERATED), owner);
            requireGeneratedShape(field);
            String sql = RecordCodec.deserializeFromBytes(
                Base64.getDecoder().decode(values.get(GENERATED)), SqlExpression.class).sql();
            try {
                GeneratedExpressionGrammar.validate(sql);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Column " + field.getName() + ": generated expression ("
                    + sql + ") is outside the grammar DuckDB reads: " + e.getMessage(), e);
            }
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
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid generated column schema: " + e.getMessage(), e);
        }
    }
}
