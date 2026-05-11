// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;

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
