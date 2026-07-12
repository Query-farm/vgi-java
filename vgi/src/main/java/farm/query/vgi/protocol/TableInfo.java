// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Mirrors the C++ {@code TableInfoSchema}. Hand-rolled by
 * {@link farm.query.vgi.internal.TableInfoSerializer} because the schema has
 * nested {@code list<list<int32>>} constraint shapes plus several optional
 * binary "inline" fields the stock {@code RecordCodec} can't currently emit
 * with the right nullability.
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
public record TableInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        String schema_name,
        byte[] columns,
        List<Integer> not_null_constraints,
        List<List<Integer>> unique_constraints,
        List<String> check_constraints,
        List<List<Integer>> primary_key_constraints,
        List<byte[]> foreign_key_constraints,
        boolean supports_insert,
        boolean supports_update,
        boolean supports_delete,
        boolean supports_returning,
        boolean supports_column_statistics,
        @Nullable byte[] scan_function,
        @Nullable byte[] insert_function,
        @Nullable byte[] update_function,
        @Nullable byte[] delete_function,
        @Nullable Long cardinality_estimate,
        @Nullable Long cardinality_max,
        @Nullable byte[] column_statistics,
        @Nullable byte[] bind_result,
        List<List<String>> required_filters) {
}
