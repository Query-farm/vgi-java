// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import java.util.List;
import java.util.Map;

/**
 * One physical source backing a multi-branch VGI table. The C++ extension's
 * optimizer rewrites a multi-branch scan into {@code UNION_ALL} of one arm per
 * branch, binding each {@code functionName} against DuckDB's function catalog.
 * Mirrors vgi-python {@code ScanBranch}.
 *
 * @param functionName  DuckDB function to call for this branch (e.g.
 *                       {@code "sequence"}, {@code "read_parquet"})
 * @param positional    positional arguments passed to the function's bind
 * @param named         named arguments passed to the function's bind
 * @param branchFilter  optional SQL expression AND'd into every scan of this
 *                       branch before pushdown; {@code null} = unconstrained
 * @param writable      declares this branch the INSERT target (at most one per
 *                       table; enforced C++-side)
 * @param sourceCatalog catalog-table branch only — companion catalog name;
 *                       {@code null} for function branches
 * @param sourceSchema  catalog-table branch only — source schema; {@code null}
 *                       for function branches
 * @param sourceTable   catalog-table branch only — base table name; its
 *                       presence selects the catalog-table kind; {@code null}
 *                       for function branches
 */
public record ScanBranch(
        String functionName,
        List<Object> positional,
        Map<String, Object> named,
        String branchFilter,
        boolean writable,
        String sourceCatalog,
        String sourceSchema,
        String sourceTable) {

    /**
     * Validates the branch and defensively copies the argument collections,
     * normalizing {@code null} to empty. A branch is either a <em>function</em>
     * branch ({@code functionName} set) or a <em>catalog-table</em> branch
     * ({@code functionName} empty and {@code sourceTable} set — it scans
     * {@code sourceCatalog.sourceSchema.sourceTable} in a companion catalog).
     *
     * @throws IllegalArgumentException if neither {@code functionName} nor
     *         {@code sourceTable} is present
     */
    public ScanBranch {
        boolean catalogTable = sourceTable != null && !sourceTable.isEmpty();
        if (!catalogTable && (functionName == null || functionName.isEmpty())) {
            throw new IllegalArgumentException(
                    "ScanBranch requires functionName (function branch) or sourceTable (catalog-table branch)");
        }
        if (functionName == null) {
            functionName = "";
        }
        positional = positional == null ? List.of() : List.copyOf(positional);
        named = named == null ? Map.of() : Map.copyOf(named);
    }

    /**
     * Read-only branch with positional args and no filter.
     *
     * @param functionName DuckDB function to call for this branch
     * @param positional   positional arguments for the function's bind
     * @return the branch
     */
    public static ScanBranch of(String functionName, Object... positional) {
        return new ScanBranch(functionName, List.of(positional), Map.of(), null, false, null, null, null);
    }

    /**
     * Read-only branch with positional args and a branch filter.
     *
     * @param functionName DuckDB function to call for this branch
     * @param branchFilter SQL expression AND'd into every scan of this branch
     * @param positional   positional arguments for the function's bind
     * @return the branch
     */
    public static ScanBranch filtered(String functionName, String branchFilter, Object... positional) {
        return new ScanBranch(functionName, List.of(positional), Map.of(), branchFilter, false, null, null, null);
    }

    /**
     * Writable branch with positional args (the table's INSERT target).
     *
     * @param functionName DuckDB function to call for this branch
     * @param positional   positional arguments for the function's bind
     * @return the writable branch
     */
    public static ScanBranch writable(String functionName, Object... positional) {
        return new ScanBranch(functionName, List.of(positional), Map.of(), null, true, null, null, null);
    }

    /**
     * Catalog-table branch (lakehouse federation): scans the base table
     * {@code sourceCatalog.sourceSchema.sourceTable} in a companion catalog
     * instead of calling a table function.
     *
     * @param sourceCatalog companion catalog name (an {@code AttachCatalogInfo} alias)
     * @param sourceSchema  source schema
     * @param sourceTable   base table name
     * @param branchFilter  optional SQL filter AND'd into every scan; {@code null} = none
     * @return the catalog-table branch
     */
    public static ScanBranch catalogTable(
            String sourceCatalog, String sourceSchema, String sourceTable, String branchFilter) {
        return new ScanBranch("", List.of(), Map.of(), branchFilter, false, sourceCatalog, sourceSchema, sourceTable);
    }
}
