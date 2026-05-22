// Copyright 2025-2026 Query.Farm LLC

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
