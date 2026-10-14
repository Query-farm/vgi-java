// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** CREATE request with one authoritative list of ConstraintDefinition IPC records. */
public record TableCreateRequest(
        byte[] attach_opaque_data,
        List<String> schema_path,
        String name,
        byte[] columns,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String on_conflict,
        List<byte[]> constraint_definitions,
        @Nullable byte[] transaction_opaque_data,
        @Nullable String comment,
        @Nullable Map<String, String> tags,
        @Nullable byte[] options,
        @Nullable byte[] layout) implements ArrowSerializableRecord {
    public TableCreateRequest {
        constraint_definitions = List.copyOf(Objects.requireNonNull(constraint_definitions, "constraint_definitions"));
    }

    public TableCreateRequest(byte[] attach_opaque_data, List<String> schema_path, String name,
            byte[] columns, String on_conflict, List<byte[]> constraint_definitions, byte[] transaction_opaque_data) {
        this(attach_opaque_data, schema_path, name, columns, on_conflict, constraint_definitions,
                transaction_opaque_data, null, null, null, null);
    }
}
