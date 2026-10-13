// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;

/** Capabilities advertised by an engine client inside a catalog attach request. */
public record ClientCapabilities(
        String engine,
        List<String> native_formats,
        List<String> catalogs,
        boolean can_stream,
        List<String> filter_encodings,
        @Nullable byte[] catalog) implements ArrowSerializableRecord {
    public ClientCapabilities(String engine, List<String> native_formats, List<String> catalogs, boolean can_stream, List<String> filter_encodings) {
        this(engine, native_formats, catalogs, can_stream, filter_encodings, null);
    }
}
