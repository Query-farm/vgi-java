// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import farm.query.vgi.write.WriteOperation;
import farm.query.vgi.write.WriteResultMode;

/**
 * A catalog table. {@code columns} is the table's Arrow schema serialized
 * as IPC bytes. {@code scanFunctionName} (with {@code scanFunctionArgs})
 * inlines the scan function so the C++ extension can skip
 * {@code catalog_table_scan_function_get}; if {@code null}, the worker
 * must implement that RPC manually.
 *
 * <p>A table is read-only unless it names write functions
 * ({@link Builder#insertFunction}, {@link Builder#updateFunction}, {@link Builder#deleteFunction}):
 * the framework then advertises them and their result modes in {@code TableInfo}, answers
 * {@code catalog_table_{insert,update,delete}_function_get}, and, when the table also carries
 * constraint records, negotiates {@code vgi.constraint.enforce.v1} (the write functions must
 * then keep those constraints). See {@link farm.query.vgi.write}.
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
 * @param constraints               provider-owned canonical constraint IPC records
 * @param definitionRevision        provider-owned definition revision, or null
 * @param statistics                per-column statistics for the optimizer
 * @param requiredFilters           required WHERE-filter groups in conjunctive
 *                                  normal form (an AND of OR-groups of raw component
 *                                  column paths); a group is satisfied when any
 *                                  one of its paths is filtered. Empty = no
 *                                  enforcement
 * @param insertFunctionName        the table in-out function backing {@code INSERT}, or
 *                                  {@code null} when the table accepts no INSERT
 * @param updateFunctionName        the table in-out function backing {@code UPDATE}, or
 *                                  {@code null}
 * @param deleteFunctionName        the table in-out function backing {@code DELETE}, or
 *                                  {@code null}
 * @param writeResultModes          per operation, the richest result mode its write function
 *                                  produces; an operation with a function but no entry
 *                                  defaults to {@link WriteResultMode#COUNT}
 * @param exposeScan                whether the scan function is also an ordinary, SQL-callable
 *                                  catalog function (default {@code false}). The framework always
 *                                  lists every function a table names, so the extension knows its
 *                                  capabilities; a scan is listed {@code table_only} (absent from
 *                                  {@code duckdb_functions()}, not callable) unless this is set.
 *                                  Write functions are always {@code table_only}. See
 *                                  {@link Builder#exposeScan(boolean)}.
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
        List<List<List<String>>> requiredFilters,
        List<byte[]> constraints,
        byte[] definitionRevision,
        String insertFunctionName,
        String updateFunctionName,
        String deleteFunctionName,
        Map<WriteOperation, WriteResultMode> writeResultModes,
        boolean exposeScan) {

    /** Validates the write declaration (see {@link #validateWrites}). */
    public CatalogTable {
        writeResultModes = writeResultModes == null ? Map.of() : Map.copyOf(writeResultModes);
        validateWrites(name, scanFunctionName, insertFunctionName, updateFunctionName, deleteFunctionName,
                writeResultModes);
    }

    /**
     * Compatibility constructor of releases before {@code exposeScan}: the scan function is
     * {@code table_only}. Parameters as for the canonical constructor.
     */
    public CatalogTable(String schema, String name, byte[] columns, String comment,
            Map<String, String> tags, String scanFunctionName, List<Object> scanFunctionPositional,
            Map<String, Object> scanFunctionNamed, Long cardinalityEstimate, Long cardinalityMax,
            boolean inlineCardinality, boolean inlineScanFunction, List<List<Integer>> primaryKey,
            List<List<Integer>> uniqueConstraints, List<String> checkConstraints, List<ForeignKey> foreignKeys,
            List<ColumnStatistics> statistics, List<List<List<String>>> requiredFilters,
            List<byte[]> constraints, byte[] definitionRevision, String insertFunctionName,
            String updateFunctionName, String deleteFunctionName,
            Map<WriteOperation, WriteResultMode> writeResultModes) {
        this(schema, name, columns, comment, tags, scanFunctionName, scanFunctionPositional,
                scanFunctionNamed, cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics, requiredFilters,
                constraints, definitionRevision, insertFunctionName, updateFunctionName, deleteFunctionName,
                writeResultModes, false);
    }

    /**
     * Compatibility constructor of releases before writable tables: a read-only table.
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
     * @param primaryKey             legacy primary-key groups
     * @param uniqueConstraints      legacy UNIQUE groups
     * @param checkConstraints       legacy CHECK expressions
     * @param foreignKeys            legacy foreign keys
     * @param statistics             per-column statistics
     * @param requiredFilters        required filter groups
     * @param constraints            canonical constraint records
     * @param definitionRevision     provider-owned definition revision, or {@code null}
     */
    public CatalogTable(String schema, String name, byte[] columns, String comment,
            Map<String, String> tags, String scanFunctionName, List<Object> scanFunctionPositional,
            Map<String, Object> scanFunctionNamed, Long cardinalityEstimate, Long cardinalityMax,
            boolean inlineCardinality, boolean inlineScanFunction, List<List<Integer>> primaryKey,
            List<List<Integer>> uniqueConstraints, List<String> checkConstraints, List<ForeignKey> foreignKeys,
            List<ColumnStatistics> statistics, List<List<List<String>>> requiredFilters,
            List<byte[]> constraints, byte[] definitionRevision) {
        this(schema, name, columns, comment, tags, scanFunctionName, scanFunctionPositional,
                scanFunctionNamed, cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics, requiredFilters,
                constraints, definitionRevision, null, null, null, Map.of());
    }

    /**
     * Rejects a write declaration vgi-python's {@code Table.__post_init__} would: a result mode
     * for an operation with no function, or UPDATE / DELETE without a scan function to supply
     * row ids.
     */
    private static void validateWrites(String name, String scan, String insert, String update, String delete,
                                       Map<WriteOperation, WriteResultMode> modes) {
        for (WriteOperation op : modes.keySet()) {
            String fn = switch (op) { case INSERT -> insert; case UPDATE -> update; case DELETE -> delete; };
            if (fn == null) {
                throw new IllegalArgumentException("Table '" + name + "': writeResultModes declares '"
                        + op.wireName() + "' without a corresponding " + op.wireName() + " function");
            }
        }
        if ((update != null || delete != null) && scan == null) {
            throw new IllegalArgumentException("Table '" + name + "': update and delete functions require"
                    + " a scan function (to provide row ids)");
        }
    }

    /**
     * The function backing {@code op}.
     *
     * @param op the operation
     * @return the write function's name, or {@code null} when the table does not accept {@code op}
     */
    public String writeFunctionName(WriteOperation op) {
        return switch (op) {
            case INSERT -> insertFunctionName;
            case UPDATE -> updateFunctionName;
            case DELETE -> deleteFunctionName;
        };
    }

    /**
     * Whether the table accepts any write.
     *
     * @return true when it declares an insert, update or delete function
     */
    public boolean writable() {
        return insertFunctionName != null || updateFunctionName != null || deleteFunctionName != null;
    }

    /**
     * {@code TableInfo.write_result_modes}: each declared operation's maximum mode
     * ({@link WriteResultMode#COUNT} unless declared otherwise).
     *
     * @return wire operation name to wire mode name
     */
    public Map<String, String> wireWriteResultModes() {
        Map<String, String> out = new LinkedHashMap<>();
        for (WriteOperation op : WriteOperation.values()) {
            if (writeFunctionName(op) != null) {
                out.put(op.wireName(), writeResultModes.getOrDefault(op, WriteResultMode.COUNT).wireName());
            }
        }
        return out;
    }

    /**
     * Returns a copy whose writes are backed by the given functions (each {@code null} for an
     * operation the table does not accept) with the given maximum result modes.
     *
     * @param insert the insert function, or {@code null}
     * @param update the update function, or {@code null}
     * @param delete the delete function, or {@code null}
     * @param modes  per operation, the richest result mode its function produces
     * @return a new table with the writes declared
     */
    public CatalogTable withWriteFunctions(String insert, String update, String delete,
                                           Map<WriteOperation, WriteResultMode> modes) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics,
                requiredFilters, constraints, definitionRevision, insert, update, delete, modes, exposeScan);
    }

    /** Compatibility constructor; legacy constraints are rejected at the wire boundary. */
    public CatalogTable(String schema, String name, byte[] columns, String comment,
            Map<String, String> tags, String scanFunctionName, List<Object> scanFunctionPositional,
            Map<String, Object> scanFunctionNamed, Long cardinalityEstimate, Long cardinalityMax,
            boolean inlineCardinality, boolean inlineScanFunction, List<List<Integer>> primaryKey,
            List<List<Integer>> uniqueConstraints, List<String> checkConstraints, List<ForeignKey> foreignKeys,
            List<ColumnStatistics> statistics, List<List<List<String>>> requiredFilters) {
        this(schema, name, columns, comment, tags, scanFunctionName, scanFunctionPositional,
                scanFunctionNamed, cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics, requiredFilters,
                List.of(), null);
    }

    /**
     * Validates and defensively copies a required-filters CNF list: rejects
     * empty OR-groups, paths and components, mirroring the vgi-python
     * descriptor validation. {@code null} normalises to an empty list.
     *
     * @param groups the CNF groups to validate, or {@code null}
     * @return an immutable, validated copy
     * @throws IllegalArgumentException if any group, path or component is malformed
     */
    static List<List<List<String>>> normalizeRequiredFilters(List<List<List<String>>> groups) {
        if (groups == null) return List.of();
        List<List<List<String>>> out = new ArrayList<>(groups.size());
        for (List<List<String>> group : groups) {
            if (group == null || group.isEmpty()) throw new IllegalArgumentException("requiredFilters must not contain empty groups");
            List<List<String>> copy = new ArrayList<>();
            for (List<String> path : group) {
                if (path == null || path.isEmpty()) throw new IllegalArgumentException("requiredFilters must not contain empty paths");
                for (String part : path) {
                    if (part == null || part.isEmpty() || part.indexOf('\0') >= 0) throw new IllegalArgumentException("invalid requiredFilters component");
                }
                if (copy.contains(path)) throw new IllegalArgumentException("duplicate requiredFilters path");
                copy.add(List.copyOf(path));
            }
            out.add(List.copyOf(copy));
        }
        return List.copyOf(out);
    }

    /**
     * Foreign-key constraint declaration. Wire shape uses column NAMES
     * (not indices) for both sides — matches the vgi-go fkSchema.
     *
     * @param fkColumns        local column names that form the foreign key
     * @param pkColumns        referenced column names in the target table
     * @param referencedSchemaPath schema path of the referenced table
     * @param referencedTable  name of the referenced table
     * @deprecated not sent under vgi.v3; use {@link ConstraintRecord.Builder#foreignKey}.
     */
    @Deprecated
    public record ForeignKey(
            List<String> fkColumns,
            List<String> pkColumns,
            List<String> referencedSchemaPath,
            String referencedTable) {
        public ForeignKey(List<String> fkColumns, List<String> pkColumns,
                String referencedSchema, String referencedTable) {
            this(fkColumns, pkColumns, List.of(referencedSchema), referencedTable);
        }
    }

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
        private boolean exposeScan;
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
        private List<List<List<String>>> requiredFilters = List.of();
        private List<byte[]> constraints = List.of();
        private byte[] definitionRevision;
        private String insertFunctionName;
        private String updateFunctionName;
        private String deleteFunctionName;
        private final Map<WriteOperation, WriteResultMode> writeResultModes =
                new java.util.EnumMap<>(WriteOperation.class);

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
         * @deprecated not sent under vgi.v3; serving a table that sets it fails. Declare a
         *             {@link ConstraintRecord} ({@link ConstraintRecord#builder}) and pass it to
         *             {@link #constraints(List, byte[])} instead.
         */
        @Deprecated
        public Builder primaryKey(List<List<Integer>> pk) {
            this.primaryKey = pk == null ? List.of() : pk;
            return this;
        }

        /**
         * Sets the UNIQUE constraint column-index groups.
         *
         * @param u column-index groups; {@code null} becomes empty
         * @return this builder
         * @deprecated not sent under vgi.v3; serving a table that sets it fails. Declare a
         *             {@link ConstraintRecord} ({@link ConstraintRecord#builder}) and pass it to
         *             {@link #constraints(List, byte[])} instead.
         */
        @Deprecated
        public Builder unique(List<List<Integer>> u) {
            this.uniqueConstraints = u == null ? List.of() : u;
            return this;
        }

        /**
         * Sets the CHECK constraint SQL expressions.
         *
         * @param c CHECK expressions; {@code null} becomes empty
         * @return this builder
         * @deprecated not sent under vgi.v3; serving a table that sets it fails. Declare a
         *             {@link ConstraintRecord} ({@link ConstraintRecord#builder}) and pass it to
         *             {@link #constraints(List, byte[])} instead.
         */
        @Deprecated
        public Builder check(List<String> c) {
            this.checkConstraints = c == null ? List.of() : c;
            return this;
        }

        /**
         * Sets the foreign-key constraints.
         *
         * @param fks foreign keys; {@code null} becomes empty
         * @return this builder
         * @deprecated not sent under vgi.v3; serving a table that sets it fails. Declare a
         *             {@link ConstraintRecord} ({@link ConstraintRecord#builder}) and pass it to
         *             {@link #constraints(List, byte[])} instead.
         */
        @Deprecated
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
         * Sets the required WHERE-filter groups (conjunctive normal form): the
         * outer list is an AND of OR-groups, each inner group a list of raw component
         * paths satisfied when any one of its paths is filtered. So
         * {@code [[["accession_number"]], [["ticker"], ["cik"]]]} means "accession_number
         * AND one of (ticker, cik)".
         *
         * @param groups required filter groups; {@code null} becomes empty
         * @return this builder
         * @throws IllegalArgumentException if any group, path or component is malformed
         */
        public Builder requiredFilters(List<List<List<String>>> groups) {
            this.requiredFilters = normalizeRequiredFilters(groups);
            return this;
        }

        /**
         * Sets the table's canonical constraint records and their provider-owned revision.
         * Each record must be owned by this table, and Arrow nullability must agree with the
         * {@code NOT_NULL}/{@code PRIMARY_KEY} records; both are checked when the table is served.
         *
         * @param records  the records, built with {@link ConstraintRecord#builder}
         * @param revision provider-owned definition revision (non-empty), or {@code null}
         * @return this builder
         */
        public Builder constraints(List<ConstraintRecord> records, byte[] revision) {
            return canonicalConstraints(ConstraintRecord.encodeAll(records), revision);
        }

        /**
         * Sets explicit canonical records, already encoded, and their provider-owned revision.
         *
         * @param records  {@code ConstraintInfo} IPC records ({@link ConstraintRecord#encode()})
         * @param revision provider-owned definition revision, or {@code null}
         * @return this builder
         */
        public Builder canonicalConstraints(List<byte[]> records, byte[] revision) {
            this.constraints = List.copyOf(records);
            this.definitionRevision = revision;
            return this;
        }

        /**
         * Makes {@code INSERT} write through {@code fnName}: a table in-out function registered
         * in this table's catalog that receives the rows to insert and emits one result batch
         * per input batch ({@link farm.query.vgi.write.WriteResults}).
         *
         * @param fnName the insert function's name
         * @return this builder
         */
        public Builder insertFunction(String fnName) {
            this.insertFunctionName = fnName;
            return this;
        }

        /**
         * Makes {@code UPDATE} write through {@code fnName}: a table in-out function registered
         * in this table's catalog. DuckDB sends it each matched row's {@code SET} columns (every
         * column with {@code RETURNING}) plus the row-id column, so the scan function must emit a
         * row id ({@link farm.query.vgi.write.WriteResults#rowIdField()}) through projection
         * pushdown.
         *
         * @param fnName the update function's name
         * @return this builder
         */
        public Builder updateFunction(String fnName) {
            this.updateFunctionName = fnName;
            return this;
        }

        /**
         * Makes {@code DELETE} write through {@code fnName}: a table in-out function registered
         * in this table's catalog that receives the row ids of the rows to delete.
         *
         * @param fnName the delete function's name
         * @return this builder
         */
        public Builder deleteFunction(String fnName) {
            this.deleteFunctionName = fnName;
            return this;
        }

        /**
         * Declares the richest result mode {@code op}'s write function produces (default
         * {@link WriteResultMode#COUNT}): {@link WriteResultMode#ROWS} to support
         * {@code RETURNING}, {@link WriteResultMode#CHANGES} for old/new images. A table that
         * supports one mode supports every lower one.
         *
         * @param op   the operation
         * @param mode its maximum mode
         * @return this builder
         */
        public Builder writeResultMode(WriteOperation op, WriteResultMode mode) {
            this.writeResultModes.put(op, mode);
            return this;
        }

        /**
         * Makes the scan function also an ordinary catalog function: listed in
         * {@code duckdb_functions()} and callable from SQL (e.g. {@code SELECT * FROM
         * cat.main.items_scan()}). Default {@code false}: the framework still lists the scan, so
         * the extension knows its capabilities (projection / filter pushdown, required secrets,
         * ...), but as {@code table_only}, reachable only through the table. Write functions are
         * always {@code table_only}; this flag does not affect them. A function registered with
         * {@link farm.query.vgi.Worker#registerUnlistedTable} stays {@code table_only} regardless.
         *
         * @param expose whether the scan is SQL-callable
         * @return this builder
         */
        public Builder exposeScan(boolean expose) {
            this.exposeScan = expose;
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
                    requiredFilters, constraints, definitionRevision,
                insertFunctionName, updateFunctionName, deleteFunctionName, writeResultModes, exposeScan);
        }
    }

    /**
     * Returns a copy whose scan function is (or is not) also an ordinary, SQL-callable
     * catalog function; see {@link Builder#exposeScan(boolean)}.
     *
     * @param expose whether the scan is SQL-callable
     * @return a new table with the flag applied
     */
    public CatalogTable withExposeScan(boolean expose) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics,
                requiredFilters, constraints, definitionRevision,
                insertFunctionName, updateFunctionName, deleteFunctionName, writeResultModes, expose);
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
                requiredFilters, constraints, definitionRevision,
                insertFunctionName, updateFunctionName, deleteFunctionName, writeResultModes, exposeScan);
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
                requiredFilters, constraints, definitionRevision,
                insertFunctionName, updateFunctionName, deleteFunctionName, writeResultModes, exposeScan);
    }

    /**
     * Returns a copy with PK/UNIQUE/CHECK/FK constraints attached.
     *
     * @param pk     primary-key column-index groups; {@code null} becomes empty
     * @param unique UNIQUE constraint column-index groups; {@code null} becomes empty
     * @param check  CHECK constraint expressions; {@code null} becomes empty
     * @param fks    foreign-key constraints; {@code null} becomes empty
     * @return a new table with the constraints applied
     * @deprecated not sent under vgi.v3; serving a table that sets them fails. Use
     *             {@link #withConstraintRecords(List, byte[])} with {@link ConstraintRecord#builder}.
     */
    @Deprecated
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
                requiredFilters, constraints, definitionRevision,
                insertFunctionName, updateFunctionName, deleteFunctionName, writeResultModes, exposeScan);
    }

    /**
     * Returns a copy with the given canonical constraint records and revision.
     *
     * @param records  the records, built with {@link ConstraintRecord#builder}
     * @param revision provider-owned definition revision (non-empty), or {@code null}
     * @return a new table carrying the records
     */
    public CatalogTable withConstraintRecords(List<ConstraintRecord> records, byte[] revision) {
        return withCanonicalConstraints(ConstraintRecord.encodeAll(records), revision);
    }

    /**
     * Returns a copy with explicit provider-owned canonical records and revision.
     *
     * @param records  {@code ConstraintInfo} IPC records ({@link ConstraintRecord#encode()})
     * @param revision provider-owned definition revision, or {@code null}
     * @return a new table carrying the records
     */
    public CatalogTable withCanonicalConstraints(List<byte[]> records, byte[] revision) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics,
                requiredFilters, List.copyOf(records), revision,
                insertFunctionName, updateFunctionName, deleteFunctionName, writeResultModes, exposeScan);
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
                requiredFilters, constraints, definitionRevision,
                insertFunctionName, updateFunctionName, deleteFunctionName, writeResultModes, exposeScan);
    }

    /**
     * Returns a copy with the required WHERE-filter groups (conjunctive normal
     * form): the outer list is an AND of OR-groups, each inner group a list of
     * raw component paths satisfied when any one of its paths is filtered. So
     * {@code [[["accession_number"]], [["ticker"], ["cik"]]]} means "accession_number
     * AND one of (ticker, cik)".
     *
     * @param groups required filter groups; {@code null} becomes empty
     * @return a new table with the required filter groups applied
     * @throws IllegalArgumentException if any group, path or component is malformed
     */
    public CatalogTable withRequiredFilters(List<List<List<String>>> groups) {
        return new CatalogTable(schema, name, columns, comment, tags,
                scanFunctionName, scanFunctionPositional, scanFunctionNamed,
                cardinalityEstimate, cardinalityMax, inlineCardinality, inlineScanFunction,
                primaryKey, uniqueConstraints, checkConstraints, foreignKeys, statistics,
                normalizeRequiredFilters(groups), constraints, definitionRevision,
                insertFunctionName, updateFunctionName, deleteFunctionName, writeResultModes, exposeScan);
    }
}
