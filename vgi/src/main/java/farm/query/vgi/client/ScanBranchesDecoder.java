// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.catalog.ColumnStatistics;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Decode the {@code ScanBranchesResult} bytes a worker returns from
 * {@code catalog_table_scan_branches_get}.
 *
 * <p>The outer stream is one row {@code {branches: list<binary>,
 * required_extensions: list<utf8>}}; each branch is itself a one-row IPC
 * stream. Fields are matched by name against the sender's own schema, so a
 * pre-3.2 sender (no {@code row_count} / {@code column_statistics}) decodes
 * with those absent.</p>
 *
 * <p>Zero branches is returned as-is: it is legal for a table whose
 * {@code TableInfo.scan_planning} is {@code "filtered"} (the scan produces no
 * rows), and a protocol error only for a bind-time table — which the caller,
 * knowing the table's mode, decides.</p>
 *
 * <pre>{@code
 * byte[] bytes = vgi.catalog_table_scan_branches_get(
 *         attach, schemaPath, "events", null, null, null, pushdownFiltersOrNull);
 * ScanBranchesDecoder.Result result = ScanBranchesDecoder.decode(bytes);
 * }</pre>
 */
public final class ScanBranchesDecoder {

    private ScanBranchesDecoder() {}

    /**
     * A decoded {@code ScanBranchesResult}.
     *
     * @param branches           the branches, possibly empty
     * @param requiredExtensions the DuckDB extensions the branches need
     */
    public record Result(List<Branch> branches, List<String> requiredExtensions) {}

    /**
     * One decoded branch. Exactly one of {@code functionName} (non-empty),
     * {@code sourceTable} or {@code formatName} names its kind.
     *
     * @param functionName     function branch: the table function to call; empty otherwise
     * @param arguments        the raw {@code arguments} bytes (decode with {@link ScanFunctionArguments})
     * @param branchFilter     SQL expression AND-ed into every scan of the branch, or {@code null}
     * @param writable         whether the branch is the table's INSERT target
     * @param sourceCatalog    catalog-table branch: the companion catalog, or {@code null}
     * @param sourceSchemaPath catalog-table branch: the source schema path, or {@code null}
     * @param sourceTable      catalog-table branch: the base table, or {@code null}
     * @param formatName       format branch: the format to read, or {@code null}
     * @param formatLocations  format branch: the locations to read; empty otherwise
     * @param formatOptions    format branch: raw reader-option bytes, or {@code null}
     * @param schemaPath       function branch: the schema of the named VGI function, or {@code null}
     * @param rowCount         protocol 3.2: estimated rows before any predicate, or {@code null}
     * @param columnStatistics protocol 3.2: conservative per-column claims, or {@code null} when absent
     */
    public record Branch(
            String functionName,
            byte[] arguments,
            String branchFilter,
            boolean writable,
            String sourceCatalog,
            List<String> sourceSchemaPath,
            String sourceTable,
            String formatName,
            List<String> formatLocations,
            byte[] formatOptions,
            List<String> schemaPath,
            Long rowCount,
            List<ColumnStatistics> columnStatistics) {

        /**
         * The decoded positional and named arguments.
         *
         * @return the arguments
         */
        public ScanFunctionArguments.Decoded decodedArguments() {
            return ScanFunctionArguments.decode(arguments);
        }
    }

    /**
     * Decode a {@code catalog_table_scan_branches_get} result.
     *
     * @param bytes the result bytes
     * @return the branches and required extensions
     * @throws IllegalStateException if the bytes are missing or malformed
     */
    public static Result decode(byte[] bytes) {
        Map<String, Object> row = readRow(bytes, "ScanBranchesResult");
        List<Branch> branches = new ArrayList<>();
        for (Object item : list(row, "branches")) {
            if (!(item instanceof byte[] b)) {
                throw new IllegalStateException("ScanBranchesResult.branches holds a null entry");
            }
            branches.add(decodeBranch(b));
        }
        return new Result(Collections.unmodifiableList(branches), strings(row, "required_extensions"));
    }

    /**
     * Decode one serialized branch.
     *
     * @param bytes the branch's one-row IPC stream
     * @return the branch
     * @throws IllegalStateException if the bytes are missing or malformed
     */
    public static Branch decodeBranch(byte[] bytes) {
        Map<String, Object> row = readRow(bytes, "ScanBranch");
        Object rowCount = row.get("row_count");
        if (rowCount != null && ((Number) rowCount).longValue() < 0) {
            throw new IllegalStateException("ScanBranch.row_count must be >= 0, got " + rowCount);
        }
        byte[] stats = (byte[]) row.get("column_statistics");
        Object functionName = row.get("function_name");
        return new Branch(
                functionName == null ? "" : functionName.toString(),
                (byte[]) row.get("arguments"),
                str(row, "branch_filter"),
                Boolean.TRUE.equals(row.get("writable")),
                str(row, "source_catalog"),
                row.get("source_schema_path") == null ? null : strings(row, "source_schema_path"),
                str(row, "source_table"),
                str(row, "format_name"),
                strings(row, "format_locations"),
                (byte[]) row.get("format_options"),
                row.get("schema_path") == null ? null : strings(row, "schema_path"),
                rowCount == null ? null : ((Number) rowCount).longValue(),
                stats == null ? null : ColumnStatisticsDecoder.decode(stats));
    }

    private static Map<String, Object> readRow(byte[] bytes, String what) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalStateException(what + ": empty bytes");
        }
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(bytes), Allocators.root())) {
            if (r.readNextBatch() == null) throw new IllegalStateException(what + " carried no batch");
            VectorSchemaRoot root = r.root();
            if (root.getRowCount() != 1) {
                throw new IllegalStateException(what + " must be a single row, got " + root.getRowCount());
            }
            return Marshalling.decodeRow(root, r.dictionaryProvider(), r.wireSchema());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(what + " decode failed", e);
        }
    }

    private static String str(Map<String, Object> row, String field) {
        Object v = row.get(field);
        return v == null ? null : v.toString();
    }

    private static List<?> list(Map<String, Object> row, String field) {
        Object v = row.get(field);
        return v instanceof List<?> l ? l : List.of();
    }

    private static List<String> strings(Map<String, Object> row, String field) {
        List<String> out = new ArrayList<>();
        for (Object o : list(row, field)) out.add(o == null ? null : o.toString());
        return Collections.unmodifiableList(out);
    }
}
