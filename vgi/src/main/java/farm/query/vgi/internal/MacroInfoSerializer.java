// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;

import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import java.io.ByteArrayOutputStream;
import java.util.Map;

/** Encodes the entire canonical family while preserving nullable, sorted tags. */
public final class MacroInfoSerializer {
    private MacroInfoSerializer() {}

    public static byte[] serialize(MacroInfo info) {
        var row = RecordCodec.toRowMap(info);
        // The RPC generic map writer does not support nullable map values.
        // Keep its nested-record support and use the catalog's existing map writer.
        row.put("tags", Map.of());
        try (var encoded = Marshalling.encodeRowForWire(
                SchemaDerivation.schemaForRecord(MacroInfo.class), row, Allocators.root());
             var bytes = new ByteArrayOutputStream();
             var writer = new IpcStreamWriter(bytes)) {
            IpcStructBuilder.writeMap(encoded.root().getVector("tags"), info.tags());
            writer.writeBatch(encoded.root(), null, encoded.provider());
            writer.writeEos();
            return bytes.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("MacroInfo serialization failed", e);
        }
    }
}
