// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/** Serialized foreign-key constraint carried inside {@link TableInfo}. */
public record ForeignKeyInfo(
        List<String> fk_columns,
        List<String> pk_columns,
        String referenced_table,
        List<String> referenced_schema_path) implements ArrowSerializableRecord {

    /**
     * Reads a {@code null} list/map component as empty: those columns are
     * non-nullable on the wire, and the record codec would otherwise write a
     * null into them, which a strict client rejects.
     */
    public ForeignKeyInfo {
        fk_columns = fk_columns == null ? List.of() : fk_columns;
        pk_columns = pk_columns == null ? List.of() : pk_columns;
        referenced_schema_path = referenced_schema_path == null ? List.of() : referenced_schema_path;
    }
}
