// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
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
 * <p>Schema-only serialisation goes via {@link MessageSerializer} so we don't
 * have to instantiate a {@link org.apache.arrow.vector.VectorSchemaRoot} —
 * dictionary-encoded fields, struct types, etc. all work without forcing the
 * caller to also provide vectors and a {@link DictionaryProvider}.
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
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
             ArrowStreamReader reader = new ArrowStreamReader(in, Allocators.root())) {
            return reader.getVectorSchemaRoot().getSchema();
        } catch (Exception e) {
            throw new RuntimeException("deserializeSchema failed", e);
        }
    }
}
