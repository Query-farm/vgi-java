// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static farm.query.vgi.internal.IpcStructBuilder.BINARY;
import static farm.query.vgi.internal.IpcStructBuilder.UTF8;
import static farm.query.vgi.internal.IpcStructBuilder.dict;
import static farm.query.vgi.internal.IpcStructBuilder.listOfPrim;
import static farm.query.vgi.internal.IpcStructBuilder.mapUtf8Utf8;
import static farm.query.vgi.internal.IpcStructBuilder.nonNull;
import static farm.query.vgi.internal.IpcStructBuilder.nullable;
import static farm.query.vgi.internal.IpcStructBuilder.registerDict;
import static farm.query.vgi.internal.IpcStructBuilder.writeDictIndex;
import static farm.query.vgi.internal.IpcStructBuilder.writeMap;
import static farm.query.vgi.internal.IpcStructBuilder.writeStringList;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarBinarySafe;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarChar;

/**
 * Serialiser for {@link MacroInfo}. The C++ extension's {@code MacroInfoSchema}
 * expects {@code macro_type} as {@code dictionary<int16, utf8>} and
 * {@code parameter_default_values} as non-nullable {@code binary} (empty bytes
 * when absent).
 */
public final class MacroInfoSerializer {

    private MacroInfoSerializer() {}

    private static final long DICT_MACRO_TYPE = 100L;
    private static final List<String> MACRO_TYPE_VALUES = List.of("scalar", "table");

    private static final Schema SCHEMA = new Schema(List.of(
            nullable("comment", UTF8),
            mapUtf8Utf8("tags"),
            nonNull("name", UTF8),
            nonNull("schema_name", UTF8),
            dict("macro_type", DICT_MACRO_TYPE, false),
            listOfPrim("parameters", UTF8),
            nonNull("parameter_default_values", BINARY),
            nonNull("definition", UTF8)));

    public static byte[] serialize(MacroInfo info) {
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        registerDict(provider, Allocators.root(), DICT_MACRO_TYPE, MACRO_TYPE_VALUES);

        return IpcStructBuilder.build(SCHEMA, provider, v -> {
            writeVarChar(v.get("comment"), info.comment());
            writeMap(v.get("tags"), info.tags());
            writeVarChar(v.get("name"), info.name());
            writeVarChar(v.get("schema_name"), info.schema_name());
            writeDictIndex(v.get("macro_type"), info.macro_type(), MACRO_TYPE_VALUES, "macro_type");
            writeStringList(v.get("parameters"), info.parameters());
            writeVarBinarySafe(v.get("parameter_default_values"), info.parameter_default_values());
            writeVarChar(v.get("definition"), info.definition());
        });
    }
}
