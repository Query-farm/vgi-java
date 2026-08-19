// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Mirrors the C++ {@code TableInfoSchema}. Hand-rolled by
 * {@link farm.query.vgi.internal.TableInfoSerializer}: the wire wants a row
 * value written as null into columns the schema declares non-nullable
 * (the cardinality pair), which is not something a schema-derived writer will
 * do for you. The component annotations below still describe the wire shape
 * exactly, and that is enforced — the serialiser is compared against this
 * declaration field-by-field by {@code WireRecordSchemaConformanceTest}.
 *
 * @param comment                     optional table comment, or {@code null}.
 * @param tags                        arbitrary key/value metadata tags.
 * @param name                        table name.
 * @param schema_name                 owning schema name.
 * @param columns                     IPC-encoded column schema.
 * @param not_null_constraints        column indices with NOT NULL constraints.
 * @param unique_constraints          column-index groups forming UNIQUE constraints.
 * @param check_constraints           CHECK constraint expressions.
 * @param primary_key_constraints     column-index groups forming the primary key.
 * @param foreign_key_constraints     IPC-encoded foreign-key constraint definitions.
 * @param supports_insert             whether the table supports INSERT.
 * @param supports_update             whether the table supports UPDATE.
 * @param supports_delete             whether the table supports DELETE.
 * @param supports_returning          whether DML supports RETURNING.
 * @param supports_column_statistics  whether per-column statistics are available.
 * @param scan_function               IPC-encoded scan function descriptor, or {@code null}.
 * @param insert_function             IPC-encoded insert function descriptor, or {@code null}.
 * @param update_function             IPC-encoded update function descriptor, or {@code null}.
 * @param delete_function             IPC-encoded delete function descriptor, or {@code null}.
 * @param cardinality_estimate        estimated row count, or {@code null}.
 * @param cardinality_max             upper-bound row count, or {@code null}.
 * @param column_statistics           IPC-encoded inline column statistics, or {@code null}.
 * @param bind_result                 IPC-encoded cached bind result, or {@code null}.
 * @param required_filters            required WHERE-filter groups in conjunctive
 *                                    normal form: an AND of OR-groups, each inner
 *                                    group a list of dotted column paths satisfied
 *                                    when any one of its paths has a filter. Empty
 *                                    means no enforcement. Trailing wire field.
 */
// The annotations below are statements about the WIRE COLUMN, not about
// whether a Java caller may pass null — several of these components are
// routinely null and are written as empty bytes or as a row-level null under a
// non-nullable column. Getting that distinction backwards is not cosmetic: a
// consumer that derives its reader from this declaration looks for a column
// shape no worker sends and rejects the whole batch ("out-of-date Apache Arrow
// schema"), which is how a wrongly-@Nullable PlanResponse failed. The
// authority is TableInfoSchema in the C++ extension's generated schemas, and
// TableInfoSerializer is checked against this declaration field-by-field by
// WireRecordSchemaConformanceTest.
public record TableInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        String schema_name,
        byte[] columns,
        // int32 on the wire, not the int64 a bare Integer would derive to:
        // these are DuckDB column indices and the C++ side reads them as
        // int32. The override recurses through the nested lists.
        @ArrowField(ArrowFieldType.INT32) List<Integer> not_null_constraints,
        @ArrowField(ArrowFieldType.INT32) List<List<Integer>> unique_constraints,
        List<String> check_constraints,
        @ArrowField(ArrowFieldType.INT32) List<List<Integer>> primary_key_constraints,
        List<byte[]> foreign_key_constraints,
        boolean supports_insert,
        boolean supports_update,
        boolean supports_delete,
        boolean supports_returning,
        boolean supports_column_statistics,
        // Non-null binary columns; "no such function" is empty bytes.
        byte[] scan_function,
        byte[] insert_function,
        byte[] update_function,
        byte[] delete_function,
        // Non-null int64 columns whose ROW value may still be null — see the
        // schema-vs-row-null note on TableInfoSerializer, where the C++ parser
        // reads them as optional<int64_t> and a sentinel -1 would be misread.
        Long cardinality_estimate,
        Long cardinality_max,
        byte[] column_statistics,
        byte[] bind_result,
        List<List<String>> required_filters) {
}
