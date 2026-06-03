// Copyright 2026 Query Farm LLC - https://query.farm

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
 *
 * @param schema                    owning schema name
 * @param name                      table name
 * @param columns                   the table's Arrow schema serialized as IPC bytes
 * @param comment                   free-text table comment ({@code ""} if none)
 * @param tags                      arbitrary key/value table metadata
 * @param scanFunctionName          name of the function that produces the table's
 *                                  rows, or {@code null} to require the
 *                                  {@code catalog_table_scan_function_get} RPC
 * @param scanFunctionPositional    positional arguments bound to the scan function
 * @param scanFunctionNamed         named arguments bound to the scan function
 * @param cardinalityEstimate       estimated row count, or {@code null} if unknown
 * @param cardinalityMax            upper-bound row count, or {@code null} if unknown
 * @param inlineCardinality         whether the cardinality is sent inline in
 *                                  {@code TableInfo} rather than via RPC
 * @param inlineScanFunction        whether the scan function is sent inline so the
 *                                  extension skips {@code catalog_table_scan_function_get}
 * @param primaryKey                primary-key column indices (grouped for composite keys)
 * @param uniqueConstraints         UNIQUE constraint column-index groups
 * @param checkConstraints          CHECK constraint SQL expressions
 * @param foreignKeys               foreign-key constraints
 * @param statistics                per-column statistics for the optimizer
 * @param requiredFieldFilterPaths  dotted column paths the optimizer must see in
 *                                  every scan's WHERE clause
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
        List<ColumnStatistics> statistics,
        List<String> requiredFieldFilterPaths) {

    /**
     * Foreign-key constraint declaration. Wire shape uses column NAMES
     * (not indices) for both sides — matches the vgi-go fkSchema.
     *
     * @param fkColumns        local column names that form the foreign key
     * @param pkColumns        referenced column names in the target table
     * @param referencedSchema schema of the referenced table
     * @param referencedTable  name of the referenced table
     */
    public record ForeignKey(
            List<String> fkColumns,
            List<String> pkColumns,
            String referencedSchema,
            String referencedTable) {}

    /**
     * Builds a table with no constraints, statistics, or required filter paths.
     *
     * @param schema                 owning schema name
     * @param name                   table name
     * @param columns                Arrow schema as IPC bytes
     * @param comment                table comment
     * @param tags                   table metadata tags
     * @param scanFunctionName       backing scan function name
     * @param scanFunctionPositional positional scan-function arguments
     * @param scanFunctionNamed      named scan-function arguments
     * @param cardinalityEstimate    estimated row count, or {@code null}
     * @param cardinalityMax         upper-bound row count, or {@code null}
     * @param inlineCardinality      whether cardinality is sent inline
     * @param inlineScanFunction     whether the scan function is sent inline
     * @deprecated use {@link #builder(String, String, byte[])}.
     */
    @Deprecated
    public CatalogTable(String schema, String name, byte[] columns, String comment,
                          Map<String, String> tags, String scanFunctionName,
                          List<Object> scanFunctionPositional, Map<String, Object> scanFunctionNamed,
                          Long cardinalityEstimate, Long cardinalityMax,
                          boolean inlineCardinality, boolean inlineScanFunction) {
        this(schema, name, columns, comment, tags, scanFunctionName, scanFunctionPositional,
                scanFunctionNamed, cardinalityEstimate, cardinalityMax,
                inlineCardinality, inlineScanFunction,
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
    }

    /**
     * Builds a table with constraints but no statistics or required filter paths.
     *
     * @param schema                 owning schema name
     * @param name                   table name
     * @param columns                Arrow schema as IPC bytes
     * @param comment                table comment
     * @param tags                   table metadata tags
     * @param scanFunctionName       backing scan function name
     * @param scanFunctionPositional positional scan-function arguments
     * @param scanFunctionNamed      named scan-function arguments
     * @param cardinalityEstimate    estimated row count, or {@code null}
     * @param cardinalityMax         upper-bound row count, or {@code null}
     * @param inlineCardinality      whether cardinality is sent inline
     * @param inlineScanFunction     whether the scan function is sent inline
     * @param primaryKey             primary-key column-index groups
     * @param uniqueConstraints      UNIQUE constraint column-index groups
     * @param checkConstraints       CHECK constraint SQL expressions
     * @param foreignKeys            foreign-key constraints
     * @deprecated use {@link #builder(String, String, byte[])}.
     */
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
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, List.of(), List.of());
    }

    /**
     * Convenience factory for a table whose rows come from an inlined,
     * argument-less scan function.
     *
     * @param schema       owning schema name
     * @param name         table name
     * @param columns      Arrow schema as IPC bytes
     * @param comment      table comment
     * @param scanFunction name of the backing scan function
     * @return a table backed by the named scan function, with no arguments,
     *         constraints, or statistics
     */
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
     *
     * @param schema  owning schema name
     * @param name    table name
     * @param columns the table's Arrow schema serialized as IPC bytes
     * @return a new builder seeded with the required fields
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
        private List<String> requiredFieldFilterPaths = List.of();

        Builder(String schema, String name, byte[] columns) {
            this.schema = schema;
            this.name = name;
            this.columns = columns;
        }

        /**
         * Sets the table comment.
         *
         * @param c comment text; {@code null} becomes {@code ""}
         * @return this builder
         */
        public Builder comment(String c) { this.comment = c == null ? "" : c; return this; }

        /**
         * Replaces the table's metadata tags.
         *
         * @param t tag map; {@code null} becomes empty
         * @return this builder
         */
        public Builder tags(Map<String, String> t) {
            this.tags = t == null ? Map.of() : Map.copyOf(t);
            return this;
        }

        /**
         * Adds or overwrites a single metadata tag.
         *
         * @param k tag key
         * @param v tag value
         * @return this builder
         */
        public Builder tag(String k, String v) {
            Map<String, String> copy = new LinkedHashMap<>(this.tags);
            copy.put(k, v);
            this.tags = copy;
            return this;
        }

        /**
         * Sets the backing scan function with no arguments.
         *
         * @param fnName scan function name
         * @return this builder
         */
        public Builder scanFunction(String fnName) {
            this.scanFunctionName = fnName;
            return this;
        }

        /**
         * Sets the backing scan function with bound arguments.
         *
         * @param fnName     scan function name
         * @param positional positional arguments; {@code null} becomes empty
         * @param named      named arguments; {@code null} becomes empty
         * @return this builder
         */
        public Builder scanFunction(String fnName,
                                     List<Object> positional,
                                     Map<String, Object> named) {
            this.scanFunctionName = fnName;
            this.scanFunctionPositional = positional == null ? List.of() : positional;
            this.scanFunctionNamed = named == null ? Map.of() : named;
            return this;
        }

        /**
         * Keeps the same backing function but skips the {@code TableInfo.scan_function}
         * inline so the C++ extension fires {@code catalog_table_scan_function_get}.
         *
         * @return this builder
         */
        public Builder rpcScanFunction() {
            this.inlineScanFunction = false;
            return this;
        }

        /**
         * Sets the inline cardinality estimate and maximum.
         *
         * @param estimate estimated row count
         * @param max      upper-bound row count
         * @return this builder
         */
        public Builder cardinality(long estimate, long max) {
            this.cardinalityEstimate = estimate;
            this.cardinalityMax = max;
            this.inlineCardinality = true;
            return this;
        }

        /**
         * Sets the primary-key column-index groups.
         *
         * @param pk column-index groups; {@code null} becomes empty
         * @return this builder
         */
        public Builder primaryKey(List<List<Integer>> pk) {
            this.primaryKey = pk == null ? List.of() : pk;
            return this;
        }

        /**
         * Sets the UNIQUE constraint column-index groups.
         *
         * @param u column-index groups; {@code null} becomes empty
         * @return this builder
         */
        public Builder unique(List<List<Integer>> u) {
            this.uniqueConstraints = u == null ? List.of() : u;
            return this;
        }

        /**
         * Sets the CHECK constraint SQL expressions.
         *
         * @param c CHECK expressions; {@code null} becomes empty
         * @return this builder
         */
        public Builder check(List<String> c) {
            this.checkConstraints = c == null ? List.of() : c;
            return this;
        }

        /**
         * Sets the foreign-key constraints.
         *
         * @param fks foreign keys; {@code null} becomes empty
         * @return this builder
         */
        public Builder foreignKeys(List<ForeignKey> fks) {
            this.foreignKeys = fks == null ? List.of() : fks;
            return this;
        }

        /**
         * Sets the per-column statistics.
         *
         * @param s statistics; {@code null} becomes empty
         * @return this builder
         */
        public Builder statistics(List<ColumnStatistics> s) {
            this.statistics = s == null ? List.of() : s;
            return this;
        }

        /**
         * Sets the dotted column paths the optimizer must see in any scan's WHERE.
         *
         * @param paths required filter paths; {@code null} becomes empty
         * @return this builder
         */
        public Builder requiredFieldFilterPaths(List<String> paths) {
            this.requiredFieldFilterPaths = paths == null ? List.of() : List.copyOf(paths);
            return this;
        }

        /**
         * Builds the immutable {@link CatalogTable}.
         *
         * @return the configured table
         */
        public CatalogTable build() {
            return new CatalogTable(schema, name, columns, comment, tags,
                    scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                    cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                    primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics,
                    requiredFieldFilterPaths);
        }
    }

    /**
     * Returns a copy with the inline cardinality estimate and maximum set.
     *
     * @param estimate estimated row count
     * @param max      upper-bound row count
     * @return a new table with cardinality applied
     */
    public CatalogTable withCardinality(long estimate, long max) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                estimate, max, true, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics,
                requiredFieldFilterPaths);
    }

    /**
     * Returns a copy that keeps the same backing function but skips the
     * {@code TableInfo.scan_function} inline so the C++ extension fires
     * {@code catalog_table_scan_function_get}.
     *
     * @return a new table requiring the scan-function RPC
     */
    public CatalogTable withRpcScanFunction() {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, false,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics,
                requiredFieldFilterPaths);
    }

    /**
     * Returns a copy with PK/UNIQUE/CHECK/FK constraints attached.
     *
     * @param pk     primary-key column-index groups; {@code null} becomes empty
     * @param unique UNIQUE constraint column-index groups; {@code null} becomes empty
     * @param check  CHECK constraint expressions; {@code null} becomes empty
     * @param fks    foreign-key constraints; {@code null} becomes empty
     * @return a new table with the constraints applied
     */
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
                statistics,
                requiredFieldFilterPaths);
    }

    /**
     * Returns a copy with per-column statistics attached; used by the optimizer
     * for filter elimination.
     *
     * @param stats per-column statistics; {@code null} becomes empty
     * @return a new table with statistics applied
     */
    public CatalogTable withStatistics(List<ColumnStatistics> stats) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys,
                stats == null ? List.of() : stats,
                requiredFieldFilterPaths);
    }

    /**
     * Returns a copy with the dotted column paths the optimizer must see in any
     * scan's WHERE clause.
     *
     * @param paths required filter paths; {@code null} becomes empty
     * @return a new table with the required filter paths applied
     */
    public CatalogTable withRequiredFieldFilterPaths(List<String> paths) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics,
                paths == null ? List.of() : List.copyOf(paths));
    }
}
