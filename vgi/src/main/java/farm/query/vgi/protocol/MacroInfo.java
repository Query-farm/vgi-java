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
 * @param parameter_default_values IPC-encoded 1-row batch of named-parameter defaults;
 *                                 {@code null} or empty when there are none.
 * @param definition               the macro body / SQL definition.
 * @param arguments_schema         optional Arrow schema (serialized as IPC bytes) with one
 *                                 nullable field per parameter, in {@code parameters} order;
 *                                 each field's type is the parameter's default-value type when
 *                                 known (else Arrow null), and the {@code vgi_doc} field
 *                                 metadata key carries the parameter's description (UTF-8,
 *                                 presence-only). Mirrors the per-argument doc channel
 *                                 functions expose. {@code null} when the worker supplied no
 *                                 per-parameter docs (older workers). Last in field order.
 */
public record MacroInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        String schema_name,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String macro_type,
        List<String> parameters,
        // The two binary fields below are NOT @Nullable even though a worker
        // may leave them unset: both wire COLUMNS are non-null binary
        // (MacroInfoSchema), and absence travels as empty bytes, which is what
        // MacroInfoSerializer writes for a null. Marking the columns nullable
        // here described a schema no worker sends, and a client that derived
        // its reader from this declaration would reject every macro listing.
        byte[] parameter_default_values,
        String definition,
        byte[] arguments_schema) implements ArrowSerializableRecord {
}
