// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.catalog;

import java.util.List;
import java.util.Map;

/**
 * A catalog table. {@code columns} is the table's Arrow schema serialized
 * as IPC bytes. {@code scanFunctionName} (with {@code scanFunctionArgs})
 * inlines the scan function so the C++ extension can skip
 * {@code catalog_table_scan_function_get}; if {@code null}, the worker
 * must implement that RPC manually.
 */
public record CatalogTable(
        String schema,
        String name,
        byte[] columns,
        String comment,
        Map<String, String> tags,
        String scanFunctionName,
        List<Object> scanFunctionPositional,
        Map<String, Object> scanFunctionNamed,
        Long cardinalityEstimate,
        Long cardinalityMax,
        boolean inlineCardinality,
        boolean inlineScanFunction,
        List<List<Integer>> primaryKey,
        List<List<Integer>> uniqueConstraints,
        List<String> checkConstraints,
        List<ForeignKey> foreignKeys,
        List<ColumnStatistics> statistics) {

    /** Foreign-key constraint declaration. Wire shape uses column NAMES
     *  (not indices) for both sides — matches the vgi-go fkSchema. */
    public record ForeignKey(
            List<String> fkColumns,
            List<String> pkColumns,
            String referencedSchema,
            String referencedTable) {}

    /** Backward-compat ctor — no constraints. */
    public CatalogTable(String schema, String name, byte[] columns, String comment,
                          Map<String, String> tags, String scanFunctionName,
                          List<Object> scanFunctionPositional, Map<String, Object> scanFunctionNamed,
                          Long cardinalityEstimate, Long cardinalityMax,
                          boolean inlineCardinality, boolean inlineScanFunction) {
        this(schema, name, columns, comment, tags, scanFunctionName, scanFunctionPositional,
                scanFunctionNamed, cardinalityEstimate, cardinalityMax,
                inlineCardinality, inlineScanFunction,
                List.of(), List.of(), List.of(), List.of(), List.of());
    }

    /** Backward-compat ctor — with constraints but no stats. */
    public CatalogTable(String schema, String name, byte[] columns, String comment,
                          Map<String, String> tags, String scanFunctionName,
                          List<Object> scanFunctionPositional, Map<String, Object> scanFunctionNamed,
                          Long cardinalityEstimate, Long cardinalityMax,
                          boolean inlineCardinality, boolean inlineScanFunction,
                          List<List<Integer>> primaryKey,
                          List<List<Integer>> uniqueConstraints,
                          List<String> checkConstraints,
                          List<ForeignKey> foreignKeys) {
        this(schema, name, columns, comment, tags, scanFunctionName, scanFunctionPositional,
                scanFunctionNamed, cardinalityEstimate, cardinalityMax,
                inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, List.of());
    }

    public static CatalogTable functionBacked(
            String schema, String name, byte[] columns, String comment,
            String scanFunction) {
        return new CatalogTable(schema, name, columns, comment, Map.of(),
                scanFunction, List.of(), Map.of(), null, null, false, true);
    }

    public CatalogTable withCardinality(long estimate, long max) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                estimate, max, true, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics);
    }

    /** Same backing function but skip the {@code TableInfo.scan_function}
     * inline so the C++ extension fires {@code catalog_table_scan_function_get}. */
    public CatalogTable withRpcScanFunction() {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, false,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics);
    }

    /** Attach PK/UNIQUE/CHECK/FK constraints to this table. */
    public CatalogTable withConstraints(List<List<Integer>> pk,
                                          List<List<Integer>> unique,
                                          List<String> check,
                                          List<ForeignKey> fks) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                pk == null ? List.of() : pk,
                unique == null ? List.of() : unique,
                check == null ? List.of() : check,
                fks == null ? List.of() : fks,
                statistics);
    }

    /** Attach per-column statistics; used by the optimizer for filter elimination. */
    public CatalogTable withStatistics(List<ColumnStatistics> stats) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys,
                stats == null ? List.of() : stats);
    }
}
