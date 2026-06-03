// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Mirrors the C++ {@code MacroInfoSchema}.
 *
 * <p>{@code macro_type} is a dictionary-encoded enum on the wire
 * ({@code "scalar"} or {@code "table"}); {@code parameter_default_values}
 * carries an IPC-encoded 1-row record batch holding default values for any
 * named parameters that omit a positional default.
 *
 * @param comment                  optional macro comment, or {@code null}.
 * @param tags                     arbitrary key/value metadata tags.
 * @param name                     macro name.
 * @param schema_name              owning schema name.
 * @param macro_type               dictionary-encoded macro kind ({@code "scalar"} or {@code "table"}).
 * @param parameters               positional parameter names.
 * @param parameter_default_values IPC-encoded 1-row batch of named-parameter defaults, or {@code null}.
 * @param definition               the macro body / SQL definition.
 */
public record MacroInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        String schema_name,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String macro_type,
        List<String> parameters,
        @Nullable byte[] parameter_default_values,
        String definition) implements ArrowSerializableRecord {
}
