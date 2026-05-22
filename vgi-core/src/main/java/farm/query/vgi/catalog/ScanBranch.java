// Copyright 2025-2026 Query.Farm LLC

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
 */
public record ScanBranch(
        String functionName,
        List<Object> positional,
        Map<String, Object> named,
        String branchFilter,
        boolean writable) {

    public ScanBranch {
        if (functionName == null || functionName.isEmpty()) {
            throw new IllegalArgumentException("ScanBranch.functionName is required");
        }
        positional = positional == null ? List.of() : List.copyOf(positional);
        named = named == null ? Map.of() : Map.copyOf(named);
    }

    /** Read-only branch with positional args and no filter. */
    public static ScanBranch of(String functionName, Object... positional) {
        return new ScanBranch(functionName, List.of(positional), Map.of(), null, false);
    }

    /** Read-only branch with positional args and a branch filter. */
    public static ScanBranch filtered(String functionName, String branchFilter, Object... positional) {
        return new ScanBranch(functionName, List.of(positional), Map.of(), branchFilter, false);
    }

    /** Writable branch with positional args. */
    public static ScanBranch writable(String functionName, Object... positional) {
        return new ScanBranch(functionName, List.of(positional), Map.of(), null, true);
    }
}
