// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Canonical table readback. Constraint entries are provider-owned ConstraintInfo IPC records. */
public record TableInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        List<String> schema_path,
        byte[] columns,
        List<byte[]> constraints,
        @Nullable byte[] definition_revision,
        Map<String, String> write_result_modes,
        boolean supports_column_statistics,
        @Nullable byte[] scan_function,
        @Nullable byte[] insert_function,
        @Nullable byte[] update_function,
        @Nullable byte[] delete_function,
        @Nullable Long cardinality_estimate,
        @Nullable Long cardinality_max,
        @Nullable byte[] column_statistics,
        @Nullable byte[] bind_result,
        List<List<List<String>>> required_filters,
        @Nullable byte[] options,
        @Nullable byte[] layout) implements farm.query.vgirpc.schema.ArrowSerializableRecord {
    public TableInfo {
        constraints = List.copyOf(Objects.requireNonNull(constraints, "constraints"));
    }

    public TableInfo(String comment, Map<String, String> tags, String name, List<String> schema_path,
            byte[] columns, List<byte[]> constraints, byte[] definition_revision,
            Map<String, String> write_result_modes, boolean supports_column_statistics,
            byte[] scan_function, byte[] insert_function, byte[] update_function, byte[] delete_function,
            Long cardinality_estimate, Long cardinality_max, byte[] column_statistics,
            byte[] bind_result, List<List<List<String>>> required_filters) {
        this(comment, tags, name, schema_path, columns, constraints, definition_revision,
                write_result_modes, supports_column_statistics, scan_function, insert_function,
                update_function, delete_function, cardinality_estimate, cardinality_max,
                column_statistics, bind_result, required_filters, null, null);
    }

    public String schema_name() {
        return schema_path.isEmpty() ? "" : schema_path.get(schema_path.size() - 1);
    }
}
