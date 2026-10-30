// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.TableInfo;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static farm.query.vgi.internal.IpcStructBuilder.BINARY;
import static farm.query.vgi.internal.IpcStructBuilder.BOOL;
import static farm.query.vgi.internal.IpcStructBuilder.I64;
import static farm.query.vgi.internal.IpcStructBuilder.UTF8;
import static farm.query.vgi.internal.IpcStructBuilder.listOfListOfListOfUtf8;
import static farm.query.vgi.internal.IpcStructBuilder.listOfPrim;
import static farm.query.vgi.internal.IpcStructBuilder.mapUtf8Utf8;
import static farm.query.vgi.internal.IpcStructBuilder.nonNull;
import static farm.query.vgi.internal.IpcStructBuilder.nullable;
import static farm.query.vgi.internal.IpcStructBuilder.writeBool;
import static farm.query.vgi.internal.IpcStructBuilder.writeListBinary;
import static farm.query.vgi.internal.IpcStructBuilder.writeListListListString;
import static farm.query.vgi.internal.IpcStructBuilder.writeMap;
import static farm.query.vgi.internal.IpcStructBuilder.writeNullableInt64;
import static farm.query.vgi.internal.IpcStructBuilder.writeNullableVarBinary;
import static farm.query.vgi.internal.IpcStructBuilder.writeStringList;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarBinarySafe;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarChar;

/**
 * Serialiser for {@link TableInfo}. Matches the C++ extension's
 * {@code TableInfoSchema} field-by-field.
 *
 * <p>{@code cardinality_estimate} / {@code cardinality_max} are nullable in
 * both the schema and the row: the C++ parser reads them via
 * {@code as<int64_t>}, which returns {@code optional<int64_t>} keyed on the
 * per-row null bit, and a sentinel {@code -1} would instead read as
 * {@code Some(-1)} and falsely trip the inlined-cardinality branch in
 * {@code VgiTableEntry::Bind}. So "unknown" must be a real null.</p>
 */
public final class TableInfoSerializer {

    private TableInfoSerializer() {}

    private static final Schema SCHEMA = new Schema(List.of(
            nullable("comment", UTF8),
            mapUtf8Utf8("tags"),
            nonNull("name", UTF8),
            listOfPrim("schema_path", UTF8),
            nonNull("columns", BINARY),
            listOfPrim("constraints", BINARY),
            nullable("definition_revision", BINARY),
            mapUtf8Utf8("write_result_modes"),
            nonNull("supports_column_statistics", BOOL),
            nullable("scan_function", BINARY),
            nullable("insert_function", BINARY),
            nullable("update_function", BINARY),
            nullable("delete_function", BINARY),
            nullable("cardinality_estimate", I64),
            nullable("cardinality_max", I64),
            nullable("column_statistics", BINARY),
            nullable("bind_result", BINARY),
            listOfListOfListOfUtf8("required_filters"),
            nullable("options", BINARY),
            nullable("layout", BINARY)));

    /**
     * Serialise {@code info} as the 1-row IPC stream the C++ extension reads for a catalog table.
     *
     * @param info the table descriptor to encode
     * @return IPC stream bytes matching {@code TableInfoSchema}
     */
    public static byte[] serialize(TableInfo info) {
        return serialize(info, null);
    }

    /**
     * Serialise {@code info} as it must be served: constraint rules checked (foreign-key
     * targets through {@code lookup}), Arrow nullability rewritten to match the records, and
     * legacy generated-column metadata translated.
     *
     * @param info   the table descriptor to encode
     * @param lookup the catalog's other tables, or null to skip cross-table checks
     * @return IPC stream bytes matching {@code TableInfoSchema}
     */
    public static byte[] serialize(TableInfo info, ConstraintContract.TableLookup lookup) {
        return encode(served(info, lookup));
    }

    /**
     * The table as the extension must see it: legacy generated-column metadata translated,
     * column metadata and the constraint rules checked, nullability rewritten.
     *
     * @param info   the table descriptor
     * @param lookup the catalog's other tables, or null to skip cross-table checks
     * @return the served descriptor
     */
    public static TableInfo served(TableInfo info, ConstraintContract.TableLookup lookup) {
        if (info.columns() != null && info.columns().length != 0) {
            info = ColumnExpressions.translateLegacy(info);
            ColumnMetadata.validateIpc(info.columns());
        }
        // The extension rejects the whole ATTACH on a violation; fail here, naming the table.
        info = ConstraintContract.serve(info, lookup);
        // After the nullability rewrite: a generated column a NOT_NULL record names is refused.
        if (info.columns() != null && info.columns().length != 0)
            ColumnExpressions.validateIpc(info.columns(), info.schema_path(), true);
        return info;
    }

    /** Encodes {@code info} as given, without the serve-time rules: wire round-trip tests only. */
    static byte[] encode(TableInfo info) {
        return IpcStructBuilder.build(SCHEMA, v -> {
            writeVarChar(v.get("comment"), info.comment());
            writeMap(v.get("tags"), info.tags());
            writeVarChar(v.get("name"), info.name());
            writeStringList(v.get("schema_path"), info.schema_path());
            writeVarBinarySafe(v.get("columns"), info.columns());
            writeListBinary(v.get("constraints"), info.constraints());
            writeNullableVarBinary(v.get("definition_revision"), info.definition_revision());
            writeMap(v.get("write_result_modes"), info.write_result_modes());
            writeBool(v.get("supports_column_statistics"), info.supports_column_statistics());
            writeVarBinarySafe(v.get("scan_function"), info.scan_function());
            writeVarBinarySafe(v.get("insert_function"), info.insert_function());
            writeVarBinarySafe(v.get("update_function"), info.update_function());
            writeVarBinarySafe(v.get("delete_function"), info.delete_function());
            writeNullableInt64(v.get("cardinality_estimate"), info.cardinality_estimate());
            writeNullableInt64(v.get("cardinality_max"), info.cardinality_max());
            writeVarBinarySafe(v.get("column_statistics"), info.column_statistics());
            writeVarBinarySafe(v.get("bind_result"), info.bind_result());
            writeListListListString(v.get("required_filters"), info.required_filters());
            writeNullableVarBinary(v.get("options"), info.options());
            writeNullableVarBinary(v.get("layout"), info.layout());
        });
    }
}
