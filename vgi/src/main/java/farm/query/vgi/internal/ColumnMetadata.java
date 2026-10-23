// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;

import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Reserved field metadata is validated and retained byte-for-byte. */
public final class ColumnMetadata {
    private ColumnMetadata() {}
    private static void scalarText(String text) throws Exception {
        StandardCharsets.UTF_8.newEncoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .encode(java.nio.CharBuffer.wrap(text));
    }
    private static void entry(String key, String value) {
        if (!key.equals("comment") && !key.equals("tags"))
            return;
        try {
            scalarText(value);
            if (!key.equals("tags"))
                return;
            try (var reader = new JsonReader(new StringReader(value))) {
                reader.setStrictness(Strictness.STRICT);
                var names = new HashSet<String>();
                reader.beginObject();
                while (reader.hasNext()) {
                    var name = reader.nextName();
                    scalarText(name);
                    if (!names.add(name))
                        throw new IllegalArgumentException("Duplicate column tag key");
                    if (reader.peek() != JsonToken.STRING)
                        throw new IllegalArgumentException("Column tag values must be strings");
                    scalarText(reader.nextString());
                }
                reader.endObject();
                if (reader.peek() != JsonToken.END_DOCUMENT)
                    throw new IllegalArgumentException("Trailing column tags JSON");
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid column " + key + " metadata", e);
        }
    }
    public static void validate(Schema schema) {
        for (var field : schema.getFields()) validate(field);
    }
    private static void validate(Field field) {
        if (field.getMetadata() != null)
            field.getMetadata().forEach(ColumnMetadata::entry);
        for (var child : field.getChildren()) validate(child);
    }
    /** Inspect Arrow's schema FlatBuffer before UTF-8 replacement by public readers. */
    public static void validateIpc(byte[] bytes) {
        try {
            var input = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            int prefix = input.getInt(0) == -1 ? 8 : 4, length = input.getInt(prefix - 4);
            if (length <= 0 || length > bytes.length - prefix)
                throw new IllegalArgumentException("Invalid column schema IPC");
            var message = org.apache.arrow.flatbuf.Message.getRootAsMessage(
                ByteBuffer.wrap(bytes, prefix, length).slice().order(ByteOrder.LITTLE_ENDIAN));
            if (message.headerType() != org.apache.arrow.flatbuf.MessageHeader.Schema)
                throw new IllegalArgumentException("Expected schema message");
            var schema = (org.apache.arrow.flatbuf.Schema) message.header(
                new org.apache.arrow.flatbuf.Schema());
            for (int i = 0; i < schema.fieldsLength(); i++) validateRaw(schema.fields(i), 0);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid column schema metadata", e);
        }
    }
    private static String text(ByteBuffer bytes) throws Exception {
        if (bytes == null)
            return "";
        return StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(bytes)
            .toString();
    }
    private static void validateRaw(org.apache.arrow.flatbuf.Field field, int depth)
        throws Exception {
        if (depth > 64)
            throw new IllegalArgumentException("Column metadata nesting too deep");
        var seen = new HashSet<String>();
        for (int i = 0; i < field.customMetadataLength(); i++) {
            var value = field.customMetadata(i);
            var key = text(value.keyAsByteBuffer());
            if (!seen.add(key))
                throw new IllegalArgumentException("Duplicate column metadata key");
            if (key.equals("comment") || key.equals("tags"))
                entry(key, text(value.valueAsByteBuffer()));
        }
        for (int i = 0; i < field.childrenLength(); i++) validateRaw(field.children(i), depth + 1);
    }
}
