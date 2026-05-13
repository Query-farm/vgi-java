// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Arrow IPC schema (de)serialisation helpers. VGI ships schemas as standalone
 * IPC streams (schema message + EOS) embedded as binary fields in wire DTOs.
 *
 * <p>Both directions go via {@link MessageSerializer} directly — using
 * {@code ArrowStreamReader} on the read side would convert dict-encoded
 * fields into <em>memory format</em> (type=indexType + DictionaryEncoding),
 * which is not what the wire protocol carries. DuckDB and other consumers
 * expect <em>wire format</em> (type=valueType + DictionaryEncoding); echoing
 * memory format back makes ENUM columns look like raw integers.
 */
public final class SchemaUtil {

    private SchemaUtil() {}

    public static byte[] serializeSchema(Schema schema) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             WriteChannel out = new WriteChannel(Channels.newChannel(baos))) {
            MessageSerializer.serialize(out, schema, IpcOption.DEFAULT);
            // EOS marker: 8 bytes of zero (continuation 0xFFFFFFFF + zero length).
            out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
            out.writeIntLittleEndian(0);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("serializeSchema failed", e);
        }
    }

    /**
     * Return a copy of {@code schema} where each named column's
     * {@link FieldType} metadata is merged with the entries in
     * {@code perColumn}. Columns not present in {@code perColumn} pass
     * through unchanged. Use to inject DuckDB-recognised tags like
     * {@code ARROW:extension:name=geoarrow.wkb} or VGI's per-column
     * {@code comment} attribute without rebuilding the schema by hand.
     */
    public static Schema withColumnMetadata(Schema schema, Map<String, Map<String, String>> perColumn) {
        if (perColumn == null || perColumn.isEmpty()) return schema;
        List<Field> rebuilt = new ArrayList<>(schema.getFields().size());
        for (Field f : schema.getFields()) {
            Map<String, String> add = perColumn.get(f.getName());
            if (add == null || add.isEmpty()) {
                rebuilt.add(f);
                continue;
            }
            Map<String, String> merged = new LinkedHashMap<>();
            if (f.getMetadata() != null) merged.putAll(f.getMetadata());
            merged.putAll(add);
            FieldType ft = f.getFieldType();
            rebuilt.add(new Field(f.getName(),
                    new FieldType(ft.isNullable(), ft.getType(), ft.getDictionary(), merged),
                    f.getChildren()));
        }
        return new Schema(rebuilt, schema.getCustomMetadata());
    }

    public static Schema deserializeSchema(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
            ReadChannel rc = new ReadChannel(Channels.newChannel(in));
            return MessageSerializer.deserializeSchema(rc);
        } catch (Exception e) {
            throw new RuntimeException("deserializeSchema failed", e);
        }
    }
}
