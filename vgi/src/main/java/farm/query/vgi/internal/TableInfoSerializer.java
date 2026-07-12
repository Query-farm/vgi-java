// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.TableInfo;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static farm.query.vgi.internal.IpcStructBuilder.BINARY;
import static farm.query.vgi.internal.IpcStructBuilder.BOOL;
import static farm.query.vgi.internal.IpcStructBuilder.I64;
import static farm.query.vgi.internal.IpcStructBuilder.UTF8;
import static farm.query.vgi.internal.IpcStructBuilder.I32;
import static farm.query.vgi.internal.IpcStructBuilder.listOfListOfInt32;
import static farm.query.vgi.internal.IpcStructBuilder.listOfListOfUtf8;
import static farm.query.vgi.internal.IpcStructBuilder.listOfPrim;
import static farm.query.vgi.internal.IpcStructBuilder.mapUtf8Utf8;
import static farm.query.vgi.internal.IpcStructBuilder.nonNull;
import static farm.query.vgi.internal.IpcStructBuilder.nullable;
import static farm.query.vgi.internal.IpcStructBuilder.writeBool;
import static farm.query.vgi.internal.IpcStructBuilder.writeListBinary;
import static farm.query.vgi.internal.IpcStructBuilder.writeListInt32;
import static farm.query.vgi.internal.IpcStructBuilder.writeListListInt32;
import static farm.query.vgi.internal.IpcStructBuilder.writeListListString;
import static farm.query.vgi.internal.IpcStructBuilder.writeMap;
import static farm.query.vgi.internal.IpcStructBuilder.writeNullableInt64;
import static farm.query.vgi.internal.IpcStructBuilder.writeStringList;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarBinarySafe;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarChar;

/**
 * Serialiser for {@link TableInfo}. Matches the C++ extension's
 * {@code TableInfoSchema} field-by-field.
 *
 * <p>{@code cardinality_estimate} / {@code cardinality_max} carry a deliberate
 * schema-vs-row-null mismatch: schema is non-nullable (the C++ parser asserts
 * that on read), but the per-row null bit MAY be set — the C++ parser reads
 * via {@code as<int64_t>} which returns {@code optional<int64_t>} keyed on
 * the null bit, and a real {@code -1} would mean {@code Some(-1)} and falsely
 * trip the inlined-cardinality branch in {@code VgiTableEntry::Bind}.</p>
 */
public final class TableInfoSerializer {

    private TableInfoSerializer() {}

    private static final Schema SCHEMA = new Schema(List.of(
            nullable("comment", UTF8),
            mapUtf8Utf8("tags"),
            nonNull("name", UTF8),
            nonNull("schema_name", UTF8),
            nonNull("columns", BINARY),
            listOfPrim("not_null_constraints", I32),
            listOfListOfInt32("unique_constraints"),
            listOfPrim("check_constraints", UTF8),
            listOfListOfInt32("primary_key_constraints"),
            listOfPrim("foreign_key_constraints", BINARY),
            nonNull("supports_insert", BOOL),
            nonNull("supports_update", BOOL),
            nonNull("supports_delete", BOOL),
            nonNull("supports_returning", BOOL),
            nonNull("supports_column_statistics", BOOL),
            nonNull("scan_function", BINARY),
            nonNull("insert_function", BINARY),
            nonNull("update_function", BINARY),
            nonNull("delete_function", BINARY),
            nonNull("cardinality_estimate", I64),
            nonNull("cardinality_max", I64),
            nonNull("column_statistics", BINARY),
            nonNull("bind_result", BINARY),
            listOfListOfUtf8("required_filters")));

    /**
     * Serialise {@code info} as the 1-row IPC stream the C++ extension reads for a catalog table.
     *
     * @param info the table descriptor to encode
     * @return IPC stream bytes matching {@code TableInfoSchema}
     */
    public static byte[] serialize(TableInfo info) {
        return IpcStructBuilder.build(SCHEMA, v -> {
            writeVarChar(v.get("comment"), info.comment());
            writeMap(v.get("tags"), info.tags());
            writeVarChar(v.get("name"), info.name());
            writeVarChar(v.get("schema_name"), info.schema_name());
            writeVarBinarySafe(v.get("columns"), info.columns());
            writeListInt32(v.get("not_null_constraints"), info.not_null_constraints());
            writeListListInt32(v.get("unique_constraints"), info.unique_constraints());
            writeStringList(v.get("check_constraints"), info.check_constraints());
            writeListListInt32(v.get("primary_key_constraints"), info.primary_key_constraints());
            writeListBinary(v.get("foreign_key_constraints"), info.foreign_key_constraints());
            writeBool(v.get("supports_insert"), info.supports_insert());
            writeBool(v.get("supports_update"), info.supports_update());
            writeBool(v.get("supports_delete"), info.supports_delete());
            writeBool(v.get("supports_returning"), info.supports_returning());
            writeBool(v.get("supports_column_statistics"), info.supports_column_statistics());
            writeVarBinarySafe(v.get("scan_function"), info.scan_function());
            writeVarBinarySafe(v.get("insert_function"), info.insert_function());
            writeVarBinarySafe(v.get("update_function"), info.update_function());
            writeVarBinarySafe(v.get("delete_function"), info.delete_function());
            writeNullableInt64(v.get("cardinality_estimate"), info.cardinality_estimate());
            writeNullableInt64(v.get("cardinality_max"), info.cardinality_max());
            writeVarBinarySafe(v.get("column_statistics"), info.column_statistics());
            writeVarBinarySafe(v.get("bind_result"), info.bind_result());
            writeListListString(v.get("required_filters"), info.required_filters());
        });
    }
}
