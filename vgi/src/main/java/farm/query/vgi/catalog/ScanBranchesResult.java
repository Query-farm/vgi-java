// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import java.util.List;

/**
 * The answer to {@code catalog_table_scan_branches_get}: the branches whose
 * {@code UNION ALL} forms a table's scan, plus the DuckDB extensions they need.
 *
 * <p>Zero branches is legal for a {@link ScanPlanning#FILTERED} table and means
 * the scan produces no rows. For a bind-time table the client still rejects an
 * empty list.</p>
 *
 * @param branches           the branches, in order; never {@code null}
 * @param requiredExtensions DuckDB extensions the client loads before binding any branch
 */
public record ScanBranchesResult(List<ScanBranch> branches, List<String> requiredExtensions) {

    /** Defensively copies both lists, normalizing {@code null} to empty. */
    public ScanBranchesResult {
        branches = branches == null ? List.of() : List.copyOf(branches);
        requiredExtensions = requiredExtensions == null ? List.of() : List.copyOf(requiredExtensions);
    }

    /**
     * A result with no required extensions.
     *
     * @param branches the branches
     * @return the result
     */
    public static ScanBranchesResult of(List<ScanBranch> branches) {
        return new ScanBranchesResult(branches, List.of());
    }

    /**
     * The empty result: the scan produces no rows (filtered tables only).
     *
     * @return a result with no branches and no required extensions
     */
    public static ScanBranchesResult empty() {
        return new ScanBranchesResult(List.of(), List.of());
    }
}
