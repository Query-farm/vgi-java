// Copyright 2025-2026 Query.Farm LLC

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
        @Nullable byte[] bind_result) {
}
