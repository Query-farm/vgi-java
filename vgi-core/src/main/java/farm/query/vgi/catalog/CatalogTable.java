// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.catalog;

import java.util.LinkedHashMap;
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

    /** @deprecated use {@link #builder(String, String, byte[])}. */
    @Deprecated
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

    /** @deprecated use {@link #builder(String, String, byte[])}. */
    @Deprecated
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

    /**
     * Start a {@link Builder} for a catalog table. The three required pieces
     * are the schema-name, table-name, and IPC-serialised column schema; all
     * other knobs (comment, tags, scan function, constraints, stats,
     * cardinality) are optional and set via chained methods. Prefer this over
     * calling the record's many-arg constructor directly — the canonical
     * constructor's field order has historically drifted as new wire fields
     * landed.
     */
    public static Builder builder(String schema, String name, byte[] columns) {
        return new Builder(schema, name, columns);
    }

    /** Fluent builder for {@link CatalogTable}. */
    public static final class Builder {
        private final String schema;
        private final String name;
        private final byte[] columns;
        private String comment = "";
        private Map<String, String> tags = Map.of();
        private String scanFunctionName;
        private List<Object> scanFunctionPositional = List.of();
        private Map<String, Object> scanFunctionNamed = Map.of();
        private Long cardinalityEstimate;
        private Long cardinalityMax;
        private boolean inlineCardinality;
        private boolean inlineScanFunction = true;
        private List<List<Integer>> primaryKey = List.of();
        private List<List<Integer>> uniqueConstraints = List.of();
        private List<String> checkConstraints = List.of();
        private List<ForeignKey> foreignKeys = List.of();
        private List<ColumnStatistics> statistics = List.of();

        Builder(String schema, String name, byte[] columns) {
            this.schema = schema;
            this.name = name;
            this.columns = columns;
        }

        public Builder comment(String c) { this.comment = c == null ? "" : c; return this; }

        public Builder tags(Map<String, String> t) {
            this.tags = t == null ? Map.of() : Map.copyOf(t);
            return this;
        }

        public Builder tag(String k, String v) {
            Map<String, String> copy = new LinkedHashMap<>(this.tags);
            copy.put(k, v);
            this.tags = copy;
            return this;
        }

        /** Set the backing scan function with no arguments. */
        public Builder scanFunction(String fnName) {
            this.scanFunctionName = fnName;
            return this;
        }

        public Builder scanFunction(String fnName,
                                     List<Object> positional,
                                     Map<String, Object> named) {
            this.scanFunctionName = fnName;
            this.scanFunctionPositional = positional == null ? List.of() : positional;
            this.scanFunctionNamed = named == null ? Map.of() : named;
            return this;
        }

        /** Same backing function but skip the {@code TableInfo.scan_function}
         *  inline so the C++ extension fires {@code catalog_table_scan_function_get}. */
        public Builder rpcScanFunction() {
            this.inlineScanFunction = false;
            return this;
        }

        public Builder cardinality(long estimate, long max) {
            this.cardinalityEstimate = estimate;
            this.cardinalityMax = max;
            this.inlineCardinality = true;
            return this;
        }

        public Builder primaryKey(List<List<Integer>> pk) {
            this.primaryKey = pk == null ? List.of() : pk;
            return this;
        }

        public Builder unique(List<List<Integer>> u) {
            this.uniqueConstraints = u == null ? List.of() : u;
            return this;
        }

        public Builder check(List<String> c) {
            this.checkConstraints = c == null ? List.of() : c;
            return this;
        }

        public Builder foreignKeys(List<ForeignKey> fks) {
            this.foreignKeys = fks == null ? List.of() : fks;
            return this;
        }

        public Builder statistics(List<ColumnStatistics> s) {
            this.statistics = s == null ? List.of() : s;
            return this;
        }

        public CatalogTable build() {
            return new CatalogTable(schema, name, columns, comment, tags,
                    scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                    cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                    primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics);
        }
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
