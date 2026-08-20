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
 * @param formatName    format branch only — the format to read ({@code parquet},
 *                       {@code csv}, {@code iceberg}, …). The CLIENT resolves it
 *                       to that format's reader, so a worker says what the data
 *                       IS without knowing the reader's argument spelling.
 *                       {@code null} for the other kinds
 * @param formatLocations format branch only — the paths/URIs to read. Required
 *                       when {@code formatName} is set; a format branch naming
 *                       no locations is rejected
 * @param formatOptions format branch only — reader options, which BECOME the
 *                       reader's named arguments. Empty for the other kinds
 */
public record ScanBranch(
        String functionName,
        List<Object> positional,
        Map<String, Object> named,
        String branchFilter,
        boolean writable,
        String sourceCatalog,
        String sourceSchema,
        String sourceTable,
        String formatName,
        List<String> formatLocations,
        Map<String, Object> formatOptions) {

    /**
     * Validates the branch and defensively copies the collections, normalizing
     * {@code null} to empty. A branch names its source in exactly one of three
     * ways:
     *
     * <ul>
     *   <li><em>function</em> — {@code functionName} set: call this DuckDB
     *       function with these arguments.</li>
     *   <li><em>catalog-table</em> — {@code sourceTable} set: scan
     *       {@code sourceCatalog.sourceSchema.sourceTable} in a companion
     *       catalog.</li>
     *   <li><em>format</em> — {@code formatName} + {@code formatLocations} set:
     *       read these locations as this format, and let the CLIENT resolve
     *       which reader that is.</li>
     * </ul>
     *
     * <p>Exactly one, checked here rather than at bind: a branch naming two
     * kinds is a worker bug, and catching it at bind would blame the query and
     * report it far from the thing that produced it. The C++ client enforces the
     * same three-way discriminator on the wire.</p>
     *
     * @throws IllegalArgumentException if the branch names no kind, more than
     *         one kind, or is a format branch with no locations
     */
    public ScanBranch {
        boolean function = functionName != null && !functionName.isEmpty();
        boolean catalogTable = sourceTable != null && !sourceTable.isEmpty();
        boolean format = formatName != null && !formatName.isEmpty();
        int kinds = (function ? 1 : 0) + (catalogTable ? 1 : 0) + (format ? 1 : 0);
        if (kinds == 0) {
            throw new IllegalArgumentException(
                    "ScanBranch requires functionName (function branch), sourceTable "
                    + "(catalog-table branch) or formatName (format branch)");
        }
        if (kinds > 1) {
            throw new IllegalArgumentException(
                    "ScanBranch declares more than one of functionName / sourceTable / formatName; "
                    + "these are mutually exclusive branch kinds");
        }
        if (format && (formatLocations == null || formatLocations.isEmpty())) {
            throw new IllegalArgumentException(
                    "format branch '" + formatName + "' names no locations to read");
        }
        if (functionName == null) {
            functionName = "";
        }
        positional = positional == null ? List.of() : List.copyOf(positional);
        named = named == null ? Map.of() : Map.copyOf(named);
        formatLocations = formatLocations == null ? List.of() : List.copyOf(formatLocations);
        formatOptions = formatOptions == null ? Map.of() : Map.copyOf(formatOptions);
    }

    /**
     * A format branch: read these locations as this format.
     *
     * <p>The worker says what the data IS, not how to read it — the client maps
     * the format to a reader ({@code csv} to {@code read_csv}, {@code iceberg} to
     * {@code iceberg_scan}) and to the argument shape that reader wants, so a
     * worker never tracks DuckDB's reader spellings.</p>
     *
     * @param formatName the format to read (e.g. {@code parquet}, {@code csv})
     * @param locations the paths/URIs to read; must be non-empty
     * @param options reader options, which become the reader's named arguments
     * @return the branch
     */
    public static ScanBranch format(String formatName, List<String> locations,
            Map<String, Object> options) {
        return new ScanBranch("", List.of(), Map.of(), null, false, null, null, null,
                formatName, locations, options);
    }

    /**
     * Read-only branch with positional args and no filter.
     *
     * @param functionName DuckDB function to call for this branch
     * @param positional   positional arguments for the function's bind
     * @return the branch
     */
    public static ScanBranch of(String functionName, Object... positional) {
        return new ScanBranch(functionName, List.of(positional), Map.of(), null, false, null, null, null, null, null, null);
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
        return new ScanBranch(functionName, List.of(positional), Map.of(), branchFilter, false, null, null, null, null, null, null);
    }

    /**
     * Writable branch with positional args (the table's INSERT target).
     *
     * @param functionName DuckDB function to call for this branch
     * @param positional   positional arguments for the function's bind
     * @return the writable branch
     */
    public static ScanBranch writable(String functionName, Object... positional) {
        return new ScanBranch(functionName, List.of(positional), Map.of(), null, true, null, null, null, null, null, null);
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
        return new ScanBranch("", List.of(), Map.of(), branchFilter, false, sourceCatalog, sourceSchema,
                sourceTable, null, null, null);
    }
}
