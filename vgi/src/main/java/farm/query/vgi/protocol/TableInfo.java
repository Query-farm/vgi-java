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
        @Nullable byte[] layout,
        String scan_planning) implements farm.query.vgirpc.schema.ArrowSerializableRecord {

    /** Protocol 3.2 {@code scan_planning} value: resolve the scan redirect at bind time (default). */
    public static final String SCAN_PLANNING_BIND = "bind";
    /** Protocol 3.2 {@code scan_planning} value: resolve it per scan with the query's filters. */
    public static final String SCAN_PLANNING_FILTERED = "filtered";

    /**
     * Validates the record. {@code scan_planning} defaults to {@code "bind"} when
     * {@code null}; any value other than {@code "bind"} / {@code "filtered"} is a
     * protocol error, and a {@code "filtered"} table may not inline a
     * (non-empty) {@code scan_function} (its redirect is resolved per query).
     */
    public TableInfo {
        constraints = List.copyOf(Objects.requireNonNull(constraints, "constraints"));
        if (scan_planning == null) scan_planning = SCAN_PLANNING_BIND;
        if (!SCAN_PLANNING_BIND.equals(scan_planning) && !SCAN_PLANNING_FILTERED.equals(scan_planning)) {
            throw new IllegalArgumentException(
                    "TableInfo.scan_planning must be one of [bind, filtered], got '" + scan_planning + "'");
        }
        // An empty scan_function is the serializer's spelling of "none".
        if (SCAN_PLANNING_FILTERED.equals(scan_planning) && scan_function != null && scan_function.length > 0) {
            throw new IllegalArgumentException("table '" + name + "': scan_planning='filtered' cannot be combined"
                    + " with an inlined scan_function; the scan redirect must be resolved per query with the"
                    + " query's filters");
        }
    }

    /** Pre-3.2 constructor: {@code scan_planning = "bind"}. */
    public TableInfo(String comment, Map<String, String> tags, String name, List<String> schema_path,
            byte[] columns, List<byte[]> constraints, byte[] definition_revision,
            Map<String, String> write_result_modes, boolean supports_column_statistics,
            byte[] scan_function, byte[] insert_function, byte[] update_function, byte[] delete_function,
            Long cardinality_estimate, Long cardinality_max, byte[] column_statistics,
            byte[] bind_result, List<List<List<String>>> required_filters, byte[] options, byte[] layout) {
        this(comment, tags, name, schema_path, columns, constraints, definition_revision,
                write_result_modes, supports_column_statistics, scan_function, insert_function,
                update_function, delete_function, cardinality_estimate, cardinality_max,
                column_statistics, bind_result, required_filters, options, layout, SCAN_PLANNING_BIND);
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
                column_statistics, bind_result, required_filters, null, null, SCAN_PLANNING_BIND);
    }

    /**
     * Return a copy with another {@code scan_planning} mode.
     *
     * @param mode {@code "bind"} or {@code "filtered"}
     * @return the copy
     */
    public TableInfo withScanPlanning(String mode) {
        return new TableInfo(comment, tags, name, schema_path, columns, constraints, definition_revision,
                write_result_modes, supports_column_statistics, scan_function, insert_function,
                update_function, delete_function, cardinality_estimate, cardinality_max,
                column_statistics, bind_result, required_filters, options, layout, mode);
    }

    public String schema_name() {
        return schema_path.isEmpty() ? "" : schema_path.get(schema_path.size() - 1);
    }
}
