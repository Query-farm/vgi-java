// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.protocol.ForeignKeyInfo;
import farm.query.vgirpc.marshal.RecordCodec;

/**
 * Serialises a {@link CatalogTable.ForeignKey} to wire bytes (a 1-row
 * IPC stream with schema {@code {fk_columns: list<utf8>, pk_columns:
 * list<utf8>, referenced_table: utf8, referenced_schema_path: list<utf8>}}). Mirrors
 * vgi-go's {@code serializeForeignKey}.
 */
public final class ForeignKeySerializer {

    private ForeignKeySerializer() {}

    /**
     * Serialise one foreign key to its one-row IPC wire bytes.
     *
     * @param fk the foreign key to serialise
     * @return the IPC stream bytes
     */
    public static byte[] serialize(CatalogTable.ForeignKey fk) {
        return RecordCodec.serializeToBytes(new ForeignKeyInfo(
                fk.fkColumns(), fk.pkColumns(), fk.referencedTable(), fk.referencedSchemaPath()));
    }
}
