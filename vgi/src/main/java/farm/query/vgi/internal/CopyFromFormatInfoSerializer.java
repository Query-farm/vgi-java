// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.CopyFromFormatInfo;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static farm.query.vgi.internal.IpcStructBuilder.BINARY;
import static farm.query.vgi.internal.IpcStructBuilder.BOOL;
import static farm.query.vgi.internal.IpcStructBuilder.UTF8;
import static farm.query.vgi.internal.IpcStructBuilder.mapUtf8Utf8;
import static farm.query.vgi.internal.IpcStructBuilder.nonNull;
import static farm.query.vgi.internal.IpcStructBuilder.nullable;
import static farm.query.vgi.internal.IpcStructBuilder.writeBool;
import static farm.query.vgi.internal.IpcStructBuilder.writeMap;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarBinarySafe;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarChar;

/**
 * Serialiser for {@link CopyFromFormatInfo}. Matches the C++ extension's
 * {@code CopyFromFormatInfoSchema} field-for-field (no enum-shaped fields, so a
 * plain struct without dictionaries suffices).
 */
final class CopyFromFormatInfoSerializer {

    private CopyFromFormatInfoSerializer() {}

    private static final Schema SCHEMA = new Schema(List.of(
            nullable("comment", UTF8),
            mapUtf8Utf8("tags"),
            nonNull("format_name", UTF8),
            nonNull("handler", UTF8),
            nonNull("options", BINARY),
            nonNull("direction", UTF8),
            nonNull("description", UTF8),
            nonNull("ordered", BOOL)));

    static byte[] serialize(CopyFromFormatInfo info) {
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        return IpcStructBuilder.build(SCHEMA, provider, v -> {
            writeVarChar(v.get("comment"), info.comment());
            writeMap(v.get("tags"), info.tags());
            writeVarChar(v.get("format_name"), info.format_name());
            writeVarChar(v.get("handler"), info.handler());
            writeVarBinarySafe(v.get("options"), info.options());
            writeVarChar(v.get("direction"), info.direction());
            writeVarChar(v.get("description"), info.description());
            writeBool(v.get("ordered"), info.ordered());
        });
    }
}
