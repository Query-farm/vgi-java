// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/** Serialized foreign-key constraint carried inside {@link TableInfo}. */
public record ForeignKeyInfo(
        List<String> fk_columns,
        List<String> pk_columns,
        String referenced_table,
        List<String> referenced_schema_path) implements ArrowSerializableRecord {}
