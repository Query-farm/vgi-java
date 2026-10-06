// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;

/**
 * The {@code request} of {@code catalog_table_create}: one IPC-serialized
 * batch, because the constraint shapes (lists of lists) do not fit flat RPC
 * parameters. Mirrors vgi-python's {@code TableCreateRequest}; the field order
 * and Arrow types are pinned against the generated {@code TableCreateRequest}
 * schema by {@code WireRecordSchemaConformanceTest}.
 *
 * @param attach_opaque_data      the attach handle
 * @param schema_path             the schema to create the table in
 * @param name                    the table name
 * @param columns                 the columns as a serialized Arrow schema
 * @param on_conflict             ERROR / IGNORE / REPLACE (dictionary-encoded on the wire)
 * @param not_null_constraints    column indices with a NOT NULL constraint
 * @param unique_constraints      column-index groups with a UNIQUE constraint
 * @param check_constraints       SQL CHECK expressions
 * @param primary_key_constraints column-index groups forming the primary key
 * @param foreign_key_constraints serialized foreign-key specs
 * @param transaction_opaque_data the transaction handle, or {@code null}
 */
public record TableCreateRequest(
        byte[] attach_opaque_data,
        List<String> schema_path,
        String name,
        byte[] columns,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String on_conflict,
        @ArrowField(ArrowFieldType.INT32) List<Integer> not_null_constraints,
        @ArrowField(ArrowFieldType.INT32) List<List<Integer>> unique_constraints,
        List<String> check_constraints,
        @ArrowField(ArrowFieldType.INT32) List<List<Integer>> primary_key_constraints,
        List<byte[]> foreign_key_constraints,
        @Nullable byte[] transaction_opaque_data) implements ArrowSerializableRecord {

    /** A missing list reads as empty. */
    public TableCreateRequest {
        not_null_constraints = not_null_constraints == null ? List.of() : not_null_constraints;
        unique_constraints = unique_constraints == null ? List.of() : unique_constraints;
        check_constraints = check_constraints == null ? List.of() : check_constraints;
        primary_key_constraints = primary_key_constraints == null ? List.of() : primary_key_constraints;
        foreign_key_constraints = foreign_key_constraints == null ? List.of() : foreign_key_constraints;
    }
}
