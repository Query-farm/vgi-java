// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import farm.query.vgi.protocol.CatalogObjectRef;
import farm.query.vgi.protocol.ConstraintDefinition;
import farm.query.vgi.protocol.ConstraintInfo;
import farm.query.vgi.protocol.ForeignKeyDefinition;
import farm.query.vgi.protocol.QueryEvaluationContext;
import farm.query.vgi.protocol.SqlExpression;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * One canonical constraint a catalog states about its own table: the typed form of a
 * {@code TableInfo.constraints} entry (a {@code ConstraintInfo} IPC record whose
 * {@code ref}, {@code definition}, check expression and foreign key are themselves
 * nested IPC records).
 *
 * <p>The worker attests the facts: the SDK never derives a record from column
 * nullability or legacy descriptor fields. The DuckDB extension reads only
 * {@link Enforcement#ENFORCED} + {@link Validation#ALL_ROWS} records, and only under the
 * {@value #CONSTRAINT_RECORDS_READ} read profile, which the framework advertises and
 * selects automatically for every catalog whose tables carry records. It also
 * requires, for every table, that a column's Arrow field is non-nullable exactly
 * when a {@code NOT_NULL} or {@code PRIMARY_KEY} record names its path; the framework
 * checks both rules when it serves the table.
 *
 * <pre>{@code
 * byte[] pk = ConstraintRecord.builder("data", "products", "products_pk")
 *         .primaryKey("id")
 *         .enforcement(ConstraintRecord.Enforcement.ENFORCED)
 *         .validation(ConstraintRecord.Validation.ALL_ROWS)
 *         .build().encode();
 * }</pre>
 *
 * @param schemaPath   schema path of the owning table
 * @param table        owning table name
 * @param id           stable provider-owned identity, unique within the table
 * @param kind         the constraint kind
 * @param columns      raw component column paths ({@code [["id"]]}, {@code [["bbox", "xmin"]]});
 *                     empty for {@code CHECK}
 * @param enforcement  whether the provider enforces the constraint on every write path
 * @param validation   which existing rows the provider has verified
 * @param declaredName optional SQL label ({@code CONSTRAINT <name>}), or {@code null}
 * @param checkSql     {@code CHECK} only: a DuckDB scalar expression over the table's columns
 * @param references   {@code FOREIGN_KEY} only: the referenced key
 */
public record ConstraintRecord(
        List<String> schemaPath,
        String table,
        String id,
        Kind kind,
        List<List<String>> columns,
        Enforcement enforcement,
        Validation validation,
        @Nullable String declaredName,
        @Nullable String checkSql,
        @Nullable ForeignKeyTarget references) {

    /** Read profile under which a catalog serves canonical {@code ConstraintInfo} records. */
    public static final String CONSTRAINT_RECORDS_READ = "vgi.constraint.records.read.v1";

    /**
     * Read profile for {@code CHECK} records' DuckDB scalar expressions; selectable only
     * alongside {@link #CONSTRAINT_RECORDS_READ}.
     */
    public static final String SCALAR_EXPRESSION_READ = "vgi.expression.duckdb.scalar.v1";

    /** The constraint kind ({@code ConstraintDefinition.kind}). */
    public enum Kind {
        /** One column path is never NULL. */
        NOT_NULL,
        /** The column tuple is unique and never NULL. */
        PRIMARY_KEY,
        /** The non-NULL column tuples are unique. */
        UNIQUE,
        /** A Boolean scalar expression holds for every row. */
        CHECK,
        /** The column tuple references a key of another table. */
        FOREIGN_KEY
    }

    /** Whether the authority enforces the constraint on every write path. */
    public enum Enforcement {
        /** Enforced on every write path. */
        ENFORCED,
        /** Informational only; nothing enforces it. */
        METADATA_ONLY
    }

    /** Which existing rows are known to satisfy the constraint. */
    public enum Validation {
        /** Every row the table can return satisfies it. */
        ALL_ROWS,
        /** Only rows written after it was declared. */
        FUTURE_WRITES,
        /** Never checked. */
        UNVALIDATED
    }

    /**
     * The key a {@code FOREIGN_KEY} references. The wire record states {@code MATCH SIMPLE}
     * and {@code NO ACTION} for both referential actions, the only shape DuckDB reads.
     *
     * @param schemaPath   schema path of the referenced table
     * @param table        referenced table name
     * @param columns      referenced column paths, one per local column
     * @param constraintId {@link ConstraintRecord#id} of the referenced table's
     *                     {@code PRIMARY_KEY} or {@code UNIQUE} record
     */
    public record ForeignKeyTarget(List<String> schemaPath, String table, List<List<String>> columns,
                                   String constraintId) {
        /** Validates and defensively copies. */
        public ForeignKeyTarget {
            schemaPath = checkedSchemaPath(schemaPath);
            identifier(table, "referenced table");
            columns = paths(columns);
            identifier(constraintId, "referenced constraint id");
            if (columns.isEmpty()) throw new IllegalArgumentException("foreign key references no columns");
        }
    }

    /** Validates the record's invariants and defensively copies. */
    public ConstraintRecord {
        schemaPath = checkedSchemaPath(schemaPath);
        identifier(table, "table");
        identifier(id, "constraint id");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(enforcement, "enforcement must be stated explicitly");
        Objects.requireNonNull(validation, "validation must be stated explicitly");
        columns = paths(columns);
        if (declaredName != null) identifier(declaredName, "declared name");
        if ((kind == Kind.CHECK) != (checkSql != null)) {
            throw new IllegalArgumentException("checkSql is required for CHECK and only CHECK");
        }
        if (checkSql != null && (checkSql.isEmpty() || checkSql.indexOf('\0') >= 0)) {
            throw new IllegalArgumentException("CHECK SQL must be non-empty without NUL");
        }
        if (kind == Kind.CHECK ? !columns.isEmpty() : columns.isEmpty()) {
            throw new IllegalArgumentException(kind == Kind.CHECK
                    ? "CHECK takes no column paths" : kind + " requires column paths");
        }
        if (kind == Kind.NOT_NULL && columns.size() != 1) {
            throw new IllegalArgumentException("NOT_NULL requires exactly one column path");
        }
        if ((kind == Kind.FOREIGN_KEY) != (references != null)) {
            throw new IllegalArgumentException("references is required for FOREIGN_KEY and only FOREIGN_KEY");
        }
        if (references != null && references.columns().size() != columns.size()) {
            throw new IllegalArgumentException("foreign key local and referenced arity must match");
        }
    }

    /**
     * Starts a record for a table in a one-component schema.
     *
     * @param schema owning schema name
     * @param table  owning table name
     * @param id     stable provider-owned identity, unique within the table
     * @return a builder; set the kind, enforcement and validation, then {@link Builder#build()}
     */
    public static Builder builder(String schema, String table, String id) {
        return new Builder(List.of(Objects.requireNonNull(schema, "schema")), table, id);
    }

    /**
     * Starts a record for a table under {@code schemaPath}.
     *
     * @param schemaPath schema path of the owning table
     * @param table      owning table name
     * @param id         stable provider-owned identity, unique within the table
     * @return a builder; set the kind, enforcement and validation, then {@link Builder#build()}
     */
    public static Builder builder(List<String> schemaPath, String table, String id) {
        return new Builder(schemaPath, table, id);
    }

    /**
     * Whether this record is one the DuckDB extension reads: {@code ENFORCED} and
     * {@code ALL_ROWS}.
     *
     * @return true for an enforced fact that holds for every row
     */
    public boolean isEngineFact() {
        return enforcement == Enforcement.ENFORCED && validation == Validation.ALL_ROWS;
    }

    /**
     * Encodes the record as the {@code ConstraintInfo} IPC bytes {@code TableInfo.constraints}
     * carries, nested records included.
     *
     * @return the IPC stream bytes
     */
    public byte[] encode() {
        byte[] check = checkSql == null ? null : RecordCodec.serializeToBytes(new SqlExpression(
                checkSql, SCALAR_EXPRESSION_READ, schemaPath,
                new QueryEvaluationContext("UTC", "binary", "asc", "nulls_last", true)));
        byte[] foreign = references == null ? null : RecordCodec.serializeToBytes(new ForeignKeyDefinition(
                ref("table", references.schemaPath(), references.table(), null),
                references.columns(), "SIMPLE", "NO_ACTION", "NO_ACTION"));
        byte[] definition = RecordCodec.serializeToBytes(
                new ConstraintDefinition(kind.name(), columns, declaredName, check, foreign));
        return RecordCodec.serializeToBytes(new ConstraintInfo(
                ref("constraint", schemaPath, id, table), definition, enforcement.name(), validation.name(),
                references == null ? null
                        : ref("constraint", references.schemaPath(), references.constraintId(), references.table())));
    }

    /**
     * Decodes a {@code TableInfo.constraints} entry (the inverse of {@link #encode()}).
     *
     * @param bytes a {@code ConstraintInfo} IPC record
     * @return the typed record
     * @throws IllegalArgumentException if the bytes are not a well-formed constraint record
     */
    public static ConstraintRecord decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) throw new IllegalArgumentException("empty constraint record");
        try {
            ConstraintInfo info = RecordCodec.deserializeFromBytes(bytes, ConstraintInfo.class);
            CatalogObjectRef ref = refOf(info.ref(), "constraint");
            if (ref.owner_table() == null) throw new IllegalArgumentException("constraint ref has no owner table");
            ConstraintDefinition def = RecordCodec.deserializeFromBytes(info.definition(), ConstraintDefinition.class);
            String check = def.check_expression() == null ? null
                    : RecordCodec.deserializeFromBytes(def.check_expression(), SqlExpression.class).sql();
            ForeignKeyTarget target = null;
            if (def.foreign_key() != null) {
                ForeignKeyDefinition fk = RecordCodec.deserializeFromBytes(def.foreign_key(), ForeignKeyDefinition.class);
                CatalogObjectRef table = refOf(fk.referenced_table(), "table");
                if (info.referenced_constraint() == null) {
                    throw new IllegalArgumentException("foreign key has no referenced constraint");
                }
                CatalogObjectRef key = refOf(info.referenced_constraint(), "constraint");
                target = new ForeignKeyTarget(table.schema_path(), table.name(), fk.referenced_column_paths(), key.name());
            }
            return new ConstraintRecord(ref.schema_path(), ref.owner_table(), ref.name(),
                    enumOf(Kind.class, def.kind()), def.column_paths(),
                    enumOf(Enforcement.class, info.enforcement()), enumOf(Validation.class, info.validation()),
                    def.declared_name(), check, target);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("malformed ConstraintInfo record", e);
        }
    }

    /**
     * Encodes several records, in order.
     *
     * @param records the records
     * @return their {@code ConstraintInfo} IPC bytes
     */
    public static List<byte[]> encodeAll(List<ConstraintRecord> records) {
        return records.stream().map(ConstraintRecord::encode).toList();
    }

    private static byte[] ref(String kind, List<String> schemaPath, String name, String owner) {
        return RecordCodec.serializeToBytes(new CatalogObjectRef(kind, schemaPath, name, owner));
    }

    private static CatalogObjectRef refOf(byte[] bytes, String kind) {
        CatalogObjectRef ref = RecordCodec.deserializeFromBytes(bytes, CatalogObjectRef.class);
        if (!kind.equals(ref.kind())) {
            throw new IllegalArgumentException("expected a " + kind + " ref, got " + ref.kind());
        }
        return ref;
    }

    private static <E extends Enum<E>> E enumOf(Class<E> type, String value) {
        try {
            return Enum.valueOf(type, Objects.requireNonNull(value, type.getSimpleName()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown " + type.getSimpleName() + " '" + value + "'", e);
        }
    }

    private static void identifier(String value, String what) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(what + " must be non-empty without NUL");
        }
    }

    private static List<String> checkedSchemaPath(List<String> path) {
        Objects.requireNonNull(path, "schemaPath");
        if (path.isEmpty()) throw new IllegalArgumentException("schemaPath must not be empty");
        path.forEach(c -> identifier(c, "schema path component"));
        return List.copyOf(path);
    }

    private static List<List<String>> paths(List<List<String>> paths) {
        if (paths == null) return List.of();
        var seen = new HashSet<List<String>>();
        var out = new ArrayList<List<String>>(paths.size());
        for (List<String> path : paths) {
            if (path == null || path.isEmpty()) throw new IllegalArgumentException("empty constraint column path");
            path.forEach(c -> identifier(c, "column path component"));
            List<String> copy = List.copyOf(path);
            if (!seen.add(copy)) throw new IllegalArgumentException("duplicate constraint column path " + copy);
            out.add(copy);
        }
        return List.copyOf(out);
    }

    private static List<List<String>> topLevel(String... columns) {
        return Arrays.stream(columns).map(List::of).toList();
    }

    /**
     * Builds a {@link ConstraintRecord}. Exactly one kind setter must be called, and
     * enforcement and validation must be stated: the SDK has no default attestation.
     */
    public static final class Builder {
        private final List<String> schemaPath;
        private final String table;
        private final String id;
        private Kind kind;
        private List<List<String>> columns = List.of();
        private Enforcement enforcement;
        private Validation validation;
        private String declaredName;
        private String checkSql;
        private ForeignKeyTarget references;

        private Builder(List<String> schemaPath, String table, String id) {
            this.schemaPath = schemaPath;
            this.table = table;
            this.id = id;
        }

        private Builder kind(Kind k, List<List<String>> cols) {
            if (kind != null) throw new IllegalStateException("constraint kind already set to " + kind);
            kind = k;
            columns = cols;
            return this;
        }

        /**
         * A {@code NOT_NULL} fact on one column path; nested components name fields of
         * ordinary STRUCT columns ({@code notNull("bbox", "xmin")}).
         *
         * @param path the column path components
         * @return this builder
         */
        public Builder notNull(String... path) {
            return kind(Kind.NOT_NULL, List.of(List.of(path)));
        }

        /**
         * A {@code PRIMARY_KEY} over top-level columns.
         *
         * @param columns the key's column names, in key order
         * @return this builder
         */
        public Builder primaryKey(String... columns) {
            return kind(Kind.PRIMARY_KEY, topLevel(columns));
        }

        /**
         * A {@code UNIQUE} constraint over top-level columns.
         *
         * @param columns the constraint's column names
         * @return this builder
         */
        public Builder unique(String... columns) {
            return kind(Kind.UNIQUE, topLevel(columns));
        }

        /**
         * A {@code CHECK} constraint.
         *
         * @param sql a DuckDB scalar expression over the table's columns ({@code "budget >= 0"})
         * @return this builder
         */
        public Builder check(String sql) {
            kind(Kind.CHECK, List.of());
            checkSql = Objects.requireNonNull(sql, "sql");
            return this;
        }

        /**
         * A {@code FOREIGN_KEY} from top-level local columns to a key of a table in the
         * same schema.
         *
         * @param columns           local column names
         * @param referencedTable   the referenced table
         * @param referencedColumns its key columns, one per local column
         * @param referencedId      the {@link ConstraintRecord#id} of its PRIMARY_KEY/UNIQUE record
         * @return this builder
         */
        public Builder foreignKey(List<String> columns, String referencedTable, List<String> referencedColumns,
                                  String referencedId) {
            return foreignKey(columns.stream().map(List::of).toList(), new ForeignKeyTarget(schemaPath,
                    referencedTable, referencedColumns.stream().map(List::of).toList(), referencedId));
        }

        /**
         * A {@code FOREIGN_KEY} with explicit column paths and target.
         *
         * @param columns local column paths
         * @param target  the referenced key
         * @return this builder
         */
        public Builder foreignKey(List<List<String>> columns, ForeignKeyTarget target) {
            kind(Kind.FOREIGN_KEY, columns);
            references = Objects.requireNonNull(target, "target");
            return this;
        }

        /**
         * Any kind with raw column paths, for callers mapping from their own model.
         *
         * @param kind    the constraint kind (not CHECK or FOREIGN_KEY, which carry payloads)
         * @param columns the column paths
         * @return this builder
         */
        public Builder of(Kind kind, List<List<String>> columns) {
            if (kind == Kind.CHECK || kind == Kind.FOREIGN_KEY) {
                throw new IllegalArgumentException("use check(..) or foreignKey(..) for " + kind);
            }
            return kind(kind, columns);
        }

        /**
         * The provider's enforcement attestation.
         *
         * @param enforcement whether every write path enforces the constraint
         * @return this builder
         */
        public Builder enforcement(Enforcement enforcement) {
            this.enforcement = Objects.requireNonNull(enforcement, "enforcement");
            return this;
        }

        /**
         * The provider's validation attestation.
         *
         * @param validation which existing rows are known to satisfy it
         * @return this builder
         */
        public Builder validation(Validation validation) {
            this.validation = Objects.requireNonNull(validation, "validation");
            return this;
        }

        /**
         * The optional SQL label ({@code CONSTRAINT <name>}).
         *
         * @param name the label, or {@code null}
         * @return this builder
         */
        public Builder declaredName(String name) {
            this.declaredName = name;
            return this;
        }

        /**
         * Validates and builds the record.
         *
         * @return the record
         * @throws IllegalStateException if no kind, enforcement or validation was set
         * @throws IllegalArgumentException if the record is malformed
         */
        public ConstraintRecord build() {
            if (kind == null) throw new IllegalStateException("constraint '" + id + "' has no kind");
            if (enforcement == null) throw new IllegalStateException("constraint '" + id + "' must state enforcement");
            if (validation == null) throw new IllegalStateException("constraint '" + id + "' must state validation");
            return new ConstraintRecord(schemaPath, table, id, kind, columns, enforcement, validation,
                    declaredName, checkSql, references);
        }
    }
}
