// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.CatalogProfiles;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.CatalogObjectRef;
import farm.query.vgi.protocol.ConstraintDefinition;
import farm.query.vgi.protocol.ConstraintInfo;
import farm.query.vgi.protocol.ForeignKeyDefinition;
import farm.query.vgi.protocol.QueryEvaluationContext;
import farm.query.vgi.protocol.SqlExpression;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The extension's ATTACH-time constraint rules ({@code DecodeNativeConstraints}), applied by
 * the worker before it serves a table, the nullability the served schema must carry, and the
 * read-profile negotiation that makes the records readable at all.
 */
public final class ConstraintContract {

    /** The read profiles a catalog serving constraint records advertises. */
    public static final List<String> READ_PROFILES =
            List.of(ConstraintRecord.CONSTRAINT_RECORDS_READ, ConstraintRecord.SCALAR_EXPRESSION_READ);

    private static final int MAX_BYTES = 8 * 1024 * 1024;
    private static final int MAX_RECORDS = 1024;
    private static final int MAX_PATHS = 1024;
    private static final int MAX_DEPTH = 64;
    private static final int MAX_IDENTIFIER = 4096;
    private static final int MAX_REVISION = 65536;

    /** Finds another table of the same catalog, for foreign-key targets. */
    @FunctionalInterface
    public interface TableLookup {
        /**
         * @param schemaPath the table's schema path
         * @param name       the table name
         * @return the table, or null when the catalog has no such table
         */
        TableInfo find(List<String> schemaPath, String name);
    }

    private ConstraintContract() {}

    /**
     * The read profiles a catalog serving {@code tables} must advertise.
     *
     * @param tables the catalog's tables
     * @return {@link #READ_PROFILES} when any table carries records, else empty
     */
    public static List<String> readProfilesFor(List<CatalogTable> tables) {
        if (tables == null) return List.of();
        for (CatalogTable t : tables) {
            if (t.constraints() != null && !t.constraints().isEmpty()) return READ_PROFILES;
        }
        return List.of();
    }

    /**
     * The client-offered profiles from {@code available}, sorted. The scalar-expression
     * profile is selected only alongside constraint records.
     *
     * @param available          profiles the catalog implements
     * @param clientCapabilities the request's {@code ClientCapabilities} IPC bytes, or null
     * @return the selected read profiles
     */
    public static List<String> selectReadProfiles(List<String> available, byte[] clientCapabilities) {
        return CatalogProfiles.select(new CatalogCapabilities(available, List.of(), List.of(), List.of()),
                clientCapabilities).read();
    }

    /**
     * Merges the profiles a catalog's tables require into a provider's own advertisement and
     * selection. With no required profiles the provider's decision is returned unchanged.
     *
     * @param explicit           the provider's own decision (either side may be null)
     * @param required           read profiles the served tables require
     * @param clientCapabilities the request's {@code ClientCapabilities} IPC bytes, or null
     * @return the advertisement and selection to put on the attach result
     */
    public static Worker.CatalogCapabilitySelection merge(Worker.CatalogCapabilitySelection explicit,
                                                          List<String> required, byte[] clientCapabilities) {
        if (explicit == null) explicit = new Worker.CatalogCapabilitySelection(null, null);
        if (required.isEmpty()) return explicit;
        CatalogCapabilities need = new CatalogCapabilities(required, List.of(), List.of(), List.of());
        CatalogCapabilities advertised = CatalogProfiles.union(CatalogProfiles.decode(explicit.advertised()), need);
        CatalogCapabilities selected = CatalogProfiles.union(CatalogProfiles.decode(explicit.selected()),
                CatalogProfiles.select(need, clientCapabilities));
        return new Worker.CatalogCapabilitySelection(CatalogProfiles.encode(advertised),
                CatalogProfiles.encode(selected));
    }

    /**
     * Checks a table the way the extension does at ATTACH. Equivalent to
     * {@link #serve(TableInfo, TableLookup)} without a lookup, discarding the served form.
     *
     * @param info the table about to be served
     * @throws IllegalArgumentException naming the table, constraint and column path on a violation
     */
    public static void validate(TableInfo info) {
        serve(info, null);
    }

    /**
     * The table as it must be served: every rule the extension checks at ATTACH, then the
     * Arrow schema rewritten so a field (descending through ordinary STRUCTs, skipping the
     * top-level row-id column) is non-nullable exactly when a {@code NOT_NULL} or
     * {@code PRIMARY_KEY} record names its path. Every record is an {@code ENFORCED} +
     * {@code ALL_ROWS} attestation, so a named field is served non-nullable; an unnamed
     * non-nullable field is served nullable, since nothing attests it.
     *
     * @param info   the table about to be served
     * @param lookup the catalog's other tables, for foreign-key targets; null skips the
     *               cross-table checks
     * @return {@code info}, or a copy carrying the rewritten schema
     * @throws IllegalArgumentException naming the table, constraint and column path on a violation
     */
    public static TableInfo serve(TableInfo info, TableLookup lookup) {
        String table = "'" + String.join(".", info.schema_path()) + "." + info.name() + "'";
        Schema schema = info.columns() == null || info.columns().length == 0
                ? null : SchemaUtil.deserializeSchema(info.columns());
        List<Field> fields = schema == null ? List.of() : schema.getFields();
        if (info.constraints().size() > MAX_RECORDS) {
            throw new IllegalArgumentException("Table " + table + ": more than " + MAX_RECORDS + " constraints");
        }
        if (info.definition_revision() != null && info.definition_revision().length > MAX_REVISION) {
            throw new IllegalArgumentException("Table " + table + ": definition_revision exceeds " + MAX_REVISION
                    + " bytes");
        }
        Set<List<String>> required = new HashSet<>();
        Set<String> ids = new HashSet<>();
        Set<String> labels = new HashSet<>();
        List<ConstraintRecord> records = new ArrayList<>();
        boolean primaryKey = false;
        long total = 0;
        int index = 0;
        for (byte[] bytes : info.constraints()) {
            if (bytes == null || bytes.length == 0 || bytes.length > MAX_BYTES || (total += bytes.length) > MAX_BYTES) {
                throw new IllegalArgumentException("Table " + table + ": constraints[" + index
                        + "] is empty or the constraint collection exceeds 8 MiB");
            }
            ConstraintRecord record;
            try {
                record = ConstraintRecord.decode(bytes);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Table " + table + ": constraints[" + index
                        + "] is not a ConstraintInfo record: " + e.getMessage(), e);
            }
            index++;
            String where = "Table " + table + ": constraint '" + record.id() + "'";
            if (!record.schemaPath().equals(info.schema_path()) || !record.table().equals(info.name())) {
                throw new IllegalArgumentException(where + " is owned by '" + String.join(".", record.schemaPath())
                        + "." + record.table() + "'");
            }
            if (record.schemaPath().size() != 1) {
                throw new IllegalArgumentException(where + ": DuckDB reads constraints only in one-component"
                        + " schema paths");
            }
            if (!ids.add(record.id())) {
                throw new IllegalArgumentException("Table " + table + ": duplicate constraint id '" + record.id() + "'");
            }
            if (!record.isEngineFact()) {
                throw new IllegalArgumentException(where + " is " + record.enforcement() + "/" + record.validation()
                        + "; DuckDB accepts only ENFORCED + ALL_ROWS records");
            }
            identifier(where, record.id());
            identifier(where, record.table());
            if (record.declaredName() != null) {
                identifier(where, record.declaredName());
                if (!labels.add(record.declaredName().toLowerCase(Locale.ROOT))) {
                    throw new IllegalArgumentException(where + ": duplicate declared name '" + record.declaredName()
                            + "' (names compare case-insensitively)");
                }
            }
            if (record.columns().size() > MAX_PATHS) {
                throw new IllegalArgumentException(where + ": more than " + MAX_PATHS + " column paths");
            }
            for (List<String> path : record.columns()) {
                if (path.size() > MAX_DEPTH) {
                    throw new IllegalArgumentException(where + ": column path " + String.join(".", path)
                            + " is deeper than " + MAX_DEPTH);
                }
                for (String c : path) identifier(where, c);
            }
            switch (record.kind()) {
                case NOT_NULL -> {
                    resolveNotNull(where, fields, record.columns().get(0));
                    required.add(record.columns().get(0));
                }
                case PRIMARY_KEY, UNIQUE -> {
                    topLevel(where, fields, record.columns());
                    if (record.kind() == ConstraintRecord.Kind.PRIMARY_KEY) {
                        if (primaryKey) {
                            throw new IllegalArgumentException(where + ": the table already has a PRIMARY KEY;"
                                    + " DuckDB reads at most one");
                        }
                        primaryKey = true;
                        required.addAll(record.columns());
                    }
                }
                case CHECK -> checkExpression(where, bytes, info.schema_path(), fields, record.checkSql());
                case FOREIGN_KEY -> {
                    topLevel(where, fields, record.columns());
                    foreignKeyShape(where, bytes, info.schema_path());
                }
            }
            records.add(record);
        }
        for (ConstraintRecord record : records) {
            if (record.kind() == ConstraintRecord.Kind.FOREIGN_KEY) foreignKeyTarget(table, info, records, record, lookup);
        }
        if (schema == null) return info;
        List<Field> served = relax(fields, List.of(), required);
        if (served.equals(fields)) return info;
        byte[] columns = SchemaUtil.serializeSchema(new Schema(served, schema.getCustomMetadata()));
        return new TableInfo(info.comment(), info.tags(), info.name(), info.schema_path(), columns,
                info.constraints(), info.definition_revision(), info.write_result_modes(),
                info.supports_column_statistics(), info.scan_function(), info.insert_function(),
                info.update_function(), info.delete_function(), info.cardinality_estimate(),
                info.cardinality_max(), info.column_statistics(), info.bind_result(), info.required_filters(),
                info.options(), info.layout());
    }

    /**
     * Re-owns a variant table's records to the table name it is served under (time travel
     * serves {@code foo_v_1} as {@code foo}): the record identities, and foreign keys that
     * referenced the variant itself.
     *
     * @param records the variant's {@code ConstraintInfo} IPC records
     * @param from    the variant's table name
     * @param to      the served table name
     * @return the re-owned records
     */
    public static List<byte[]> reown(List<byte[]> records, String from, String to) {
        if (records == null || records.isEmpty() || from.equals(to)) return records == null ? List.of() : records;
        List<byte[]> out = new ArrayList<>(records.size());
        for (byte[] bytes : records) {
            ConstraintInfo info = RecordCodec.deserializeFromBytes(bytes, ConstraintInfo.class);
            byte[] definition = info.definition();
            byte[] target = info.referenced_constraint();
            ConstraintDefinition def = RecordCodec.deserializeFromBytes(definition, ConstraintDefinition.class);
            if (def.foreign_key() != null && target != null) {
                ForeignKeyDefinition fk = RecordCodec.deserializeFromBytes(def.foreign_key(), ForeignKeyDefinition.class);
                CatalogObjectRef referenced = RecordCodec.deserializeFromBytes(fk.referenced_table(), CatalogObjectRef.class);
                if (referenced.name().equals(from)) {
                    byte[] table = RecordCodec.serializeToBytes(new CatalogObjectRef(referenced.kind(),
                            referenced.schema_path(), to, referenced.owner_table()));
                    definition = RecordCodec.serializeToBytes(new ConstraintDefinition(def.kind(), def.column_paths(),
                            def.declared_name(), def.check_expression(), RecordCodec.serializeToBytes(
                                    new ForeignKeyDefinition(table, fk.referenced_column_paths(), fk.match(),
                                            fk.on_update(), fk.on_delete()))));
                    target = owned(target, from, to);
                }
            }
            out.add(RecordCodec.serializeToBytes(new ConstraintInfo(owned(info.ref(), from, to), definition,
                    info.enforcement(), info.validation(), target)));
        }
        return List.copyOf(out);
    }

    private static byte[] owned(byte[] refBytes, String from, String to) {
        CatalogObjectRef ref = RecordCodec.deserializeFromBytes(refBytes, CatalogObjectRef.class);
        if (!from.equals(ref.owner_table())) return refBytes;
        return RecordCodec.serializeToBytes(new CatalogObjectRef(ref.kind(), ref.schema_path(), ref.name(), to));
    }

    // ------------------------------------------------------------------ nullability

    private static List<Field> relax(List<Field> fields, List<String> prefix, Set<List<String>> required) {
        List<Field> out = new ArrayList<>(fields.size());
        for (Field field : fields) {
            if (prefix.isEmpty() && isRowId(field)) {
                out.add(field);
                continue;
            }
            List<String> path = append(prefix, field.getName());
            boolean nullable = !required.contains(path);
            List<Field> children = ordinaryStruct(field)
                    ? relax(field.getChildren(), path, required) : field.getChildren();
            if (nullable == field.isNullable() && children.equals(field.getChildren())) {
                out.add(field);
                continue;
            }
            FieldType type = field.getFieldType();
            out.add(new Field(field.getName(),
                    new FieldType(nullable, type.getType(), type.getDictionary(), type.getMetadata()), children));
        }
        return out;
    }

    private static boolean isRowId(Field field) {
        Map<String, String> metadata = field.getMetadata();
        return metadata != null && metadata.containsKey("is_row_id");
    }

    private static boolean ordinaryStruct(Field field) {
        if (!(field.getType() instanceof ArrowType.Struct)) return false;
        Map<String, String> metadata = field.getMetadata();
        if (metadata == null) return true;
        for (String key : metadata.keySet()) {
            if (key.equals("ARROW:extension:name") || key.startsWith("vgi.logical_type.")) return false;
        }
        return true;
    }

    // ------------------------------------------------------------------ column paths

    private static Field unique(List<Field> fields, String name) {
        Field found = null;
        for (Field f : fields) {
            if (f.getName().equals(name)) {
                if (found != null) return null;
                found = f;
            }
        }
        return found;
    }

    private static void resolveNotNull(String where, List<Field> fields, List<String> path) {
        Field field = unique(fields, path.get(0));
        if (field == null || isRowId(field)) {
            throw new IllegalArgumentException(where + " names column " + String.join(".", path)
                    + ", which is not a column reachable through ordinary STRUCTs (missing, ambiguous or the row id)");
        }
        for (int depth = 1; depth < path.size(); depth++) {
            Field child = ordinaryStruct(field) ? unique(field.getChildren(), path.get(depth)) : null;
            if (child == null) {
                throw new IllegalArgumentException(where + " names column " + String.join(".", path)
                        + ", which is not a column reachable through ordinary STRUCTs");
            }
            field = child;
        }
    }

    private static void topLevel(String where, List<Field> fields, List<List<String>> paths) {
        for (List<String> path : paths) {
            if (path.size() != 1) {
                throw new IllegalArgumentException(where + " names nested column " + String.join(".", path)
                        + "; DuckDB reads nested paths only on NOT_NULL records");
            }
            Field field = unique(fields, path.get(0));
            if (field == null || isRowId(field)) {
                throw new IllegalArgumentException(where + " names column " + path.get(0)
                        + ", which is missing, ambiguous or the row id");
            }
        }
    }

    // ------------------------------------------------------------------ foreign keys

    private static void foreignKeyShape(String where, byte[] bytes, List<String> schemaPath) {
        ConstraintInfo info = RecordCodec.deserializeFromBytes(bytes, ConstraintInfo.class);
        ConstraintDefinition def = RecordCodec.deserializeFromBytes(info.definition(), ConstraintDefinition.class);
        ForeignKeyDefinition fk = RecordCodec.deserializeFromBytes(def.foreign_key(), ForeignKeyDefinition.class);
        CatalogObjectRef table = RecordCodec.deserializeFromBytes(fk.referenced_table(), CatalogObjectRef.class);
        CatalogObjectRef key = RecordCodec.deserializeFromBytes(info.referenced_constraint(), CatalogObjectRef.class);
        if (table.owner_table() != null) {
            throw new IllegalArgumentException(where + ": the referenced table ref must have no owner table");
        }
        if (!table.schema_path().equals(schemaPath) || !key.schema_path().equals(table.schema_path())
                || !table.name().equals(key.owner_table())) {
            throw new IllegalArgumentException(where + ": a FOREIGN KEY must reference an identified key of a table"
                    + " in the same schema (references '" + String.join(".", table.schema_path()) + "."
                    + table.name() + "', key owned by '" + key.owner_table() + "')");
        }
        if (!"SIMPLE".equals(fk.match())) {
            throw new IllegalArgumentException(where + ": DuckDB reads only MATCH SIMPLE foreign keys");
        }
        for (String action : List.of(fk.on_update(), fk.on_delete())) {
            if (!"NO_ACTION".equals(action) && !"RESTRICT".equals(action)) {
                throw new IllegalArgumentException(where + ": referential action " + action
                        + " is not supported; DuckDB reads only NO_ACTION and RESTRICT");
            }
        }
        for (List<String> path : fk.referenced_column_paths()) {
            if (path.size() != 1) {
                throw new IllegalArgumentException(where + ": referenced column " + String.join(".", path)
                        + " is nested; foreign keys reference top-level columns");
            }
        }
    }

    private static void foreignKeyTarget(String table, TableInfo info, List<ConstraintRecord> own,
                                         ConstraintRecord fk, TableLookup lookup) {
        ConstraintRecord.ForeignKeyTarget target = fk.references();
        String where = "Table " + table + ": constraint '" + fk.id() + "'";
        List<ConstraintRecord> candidates;
        List<Field> columns;
        if (target.table().equals(info.name())) {
            candidates = own;
            columns = null;
        } else {
            if (lookup == null) return;
            TableInfo referenced = lookup.find(target.schemaPath(), target.table());
            if (referenced == null) {
                throw new IllegalArgumentException(where + " references table '" + String.join(".", target.schemaPath())
                        + "." + target.table() + "', which the catalog does not serve");
            }
            candidates = new ArrayList<>();
            for (byte[] b : referenced.constraints()) candidates.add(ConstraintRecord.decode(b));
            Schema s = referenced.columns() == null || referenced.columns().length == 0
                    ? null : SchemaUtil.deserializeSchema(referenced.columns());
            columns = s == null ? List.of() : s.getFields();
        }
        for (ConstraintRecord key : candidates) {
            if (!key.id().equals(target.constraintId())) continue;
            if (key.kind() != ConstraintRecord.Kind.PRIMARY_KEY && key.kind() != ConstraintRecord.Kind.UNIQUE) {
                throw new IllegalArgumentException(where + " references constraint '" + key.id() + "' of '"
                        + target.table() + "', which is " + key.kind() + ", not PRIMARY_KEY or UNIQUE");
            }
            if (!new HashSet<>(key.columns()).equals(new HashSet<>(target.columns()))) {
                throw new IllegalArgumentException(where + " references columns " + target.columns() + " of '"
                        + target.table() + "', but its key '" + key.id() + "' covers " + key.columns());
            }
            if (columns != null) topLevel(where, columns, target.columns());
            return;
        }
        throw new IllegalArgumentException(where + " references constraint '" + target.constraintId() + "' of '"
                + target.table() + "', which that table does not declare");
    }

    // ------------------------------------------------------------------ CHECK

    private static void checkExpression(String where, byte[] bytes, List<String> schemaPath, List<Field> fields,
                                        String sql) {
        ConstraintInfo info = RecordCodec.deserializeFromBytes(bytes, ConstraintInfo.class);
        ConstraintDefinition def = RecordCodec.deserializeFromBytes(info.definition(), ConstraintDefinition.class);
        SqlExpression expression = RecordCodec.deserializeFromBytes(def.check_expression(), SqlExpression.class);
        if (!ConstraintRecord.SCALAR_EXPRESSION_READ.equals(expression.profile())) {
            throw new IllegalArgumentException(where + ": CHECK profile must be "
                    + ConstraintRecord.SCALAR_EXPRESSION_READ);
        }
        if (!expression.schema_path().equals(schemaPath)) {
            throw new IllegalArgumentException(where + ": CHECK expression belongs to schema "
                    + String.join(".", expression.schema_path()));
        }
        QueryEvaluationContext c = expression.evaluation_context();
        if (c == null || !"UTC".equals(c.timezone()) || !"binary".equals(c.collation())
                || !"asc".equals(c.default_order()) || !"nulls_last".equals(c.default_null_order())
                || !c.preserve_insertion_order()) {
            throw new IllegalArgumentException(where + ": CHECK requires the default evaluation context"
                    + " (UTC, binary, asc, nulls_last, preserve_insertion_order)");
        }
        try {
            new CheckParser(sql, fields).parse();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(where + ": CHECK (" + sql + ") is outside the grammar DuckDB reads: "
                    + e.getMessage() + ". It reads only one comparison (=, ==, !=, <>, <, >, <=, >=) between"
                    + " unqualified signed-integer/float columns and numeric literals");
        }
    }

    /**
     * Validates a {@code CHECK} expression against the grammar the extension's
     * {@code ParseNumericCheck} accepts over {@code fields}.
     *
     * @param sql    the expression
     * @param fields the table's top-level Arrow fields
     * @throws IllegalArgumentException when DuckDB would refuse it
     */
    public static void checkGrammar(String sql, List<Field> fields) {
        new CheckParser(sql, fields).parse();
    }

    /** One comparison of two numeric operands: {@code ParseNumericCheck} + {@code NumericOperand}. */
    private static final class CheckParser {
        private final String s;
        private final List<Field> fields;
        private int i;

        CheckParser(String sql, List<Field> fields) {
            if (sql == null || sql.isEmpty() || sql.length() > 1024 * 1024 || sql.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("empty, oversized or NUL-bearing SQL");
            }
            this.s = sql;
            this.fields = fields;
        }

        void parse() {
            comparison();
            skip();
            if (i != s.length()) throw new IllegalArgumentException("unexpected text at '" + s.substring(i) + "'");
        }

        private void comparison() {
            skip();
            int start = i;
            if (peek('(')) {
                i++;
                try {
                    comparison();
                    skip();
                    expect(')');
                    return;
                } catch (IllegalArgumentException parenthesizedOperand) {
                    i = start;
                }
            }
            operand();
            skip();
            String op = operator();
            if (op == null) throw new IllegalArgumentException("expected a comparison operator");
            operand();
            skip();
            if (i < s.length() && "=<>!".indexOf(s.charAt(i)) >= 0) {
                throw new IllegalArgumentException("chained comparisons are not one comparison");
            }
        }

        private void operand() {
            skip();
            if (peek('(')) {
                i++;
                operand();
                skip();
                expect(')');
                return;
            }
            if (i >= s.length()) throw new IllegalArgumentException("missing operand");
            char ch = s.charAt(i);
            if (ch == '-' && i + 1 < s.length() && (Character.isDigit(s.charAt(i + 1)) || s.charAt(i + 1) == '.')) {
                i++;
                number();
                return;
            }
            if (Character.isDigit(ch) || (ch == '.' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1)))) {
                number();
                return;
            }
            String name;
            if (ch == '"') {
                StringBuilder b = new StringBuilder();
                i++;
                while (true) {
                    if (i >= s.length()) throw new IllegalArgumentException("unterminated quoted identifier");
                    char c = s.charAt(i++);
                    if (c == '"') {
                        if (peek('"')) { b.append('"'); i++; } else break;
                    } else b.append(c);
                }
                name = b.toString();
            } else if (Character.isLetter(ch) || ch == '_') {
                int start = i;
                while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_'
                        || s.charAt(i) == '$')) i++;
                name = s.substring(start, i);
                String lower = name.toLowerCase(Locale.ROOT);
                if (List.of("true", "false", "null", "not", "and", "or", "is", "between", "in", "like", "case")
                        .contains(lower)) {
                    throw new IllegalArgumentException("'" + name + "' is not a numeric operand");
                }
                skip();
                if (peek('.')) throw new IllegalArgumentException("qualified column references are unsupported");
                if (peek('(')) throw new IllegalArgumentException("function calls are unsupported");
            } else {
                throw new IllegalArgumentException("'" + ch + "' does not start a numeric column or literal");
            }
            column(name);
            skip();
            if (s.startsWith("::", i)) throw new IllegalArgumentException("casts are unsupported");
        }

        private void column(String name) {
            Field found = null;
            for (Field f : fields) {
                if (f.getName().equalsIgnoreCase(name)) {
                    if (found != null) throw new IllegalArgumentException("ambiguous column " + name);
                    found = f;
                }
            }
            if (found == null || isRowId(found)) {
                throw new IllegalArgumentException("column " + name + " is missing or the row id");
            }
            if ((found.getMetadata() != null && found.getMetadata().containsKey("ARROW:extension:name"))
                    || found.getType() instanceof ArrowType.ExtensionType) {
                throw new IllegalArgumentException("column " + name + " is extension-typed");
            }
            boolean numeric = found.getType() instanceof ArrowType.Int t ? t.getIsSigned()
                    : found.getType() instanceof ArrowType.FloatingPoint f
                            && f.getPrecision() != FloatingPointPrecision.HALF;
            if (!numeric || found.getDictionary() != null) {
                throw new IllegalArgumentException("column " + name + " is not a signed integer or float column");
            }
        }

        private void number() {
            int start = i;
            while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
            if (peek('.')) {
                i++;
                while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
            }
            if (i == start || (i == start + 1 && s.charAt(start) == '.')) {
                throw new IllegalArgumentException("malformed number");
            }
            if (i < s.length() && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
                i++;
                if (peek('+') || peek('-')) i++;
                int digits = i;
                while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
                if (i == digits) throw new IllegalArgumentException("malformed exponent");
            }
            if (i < s.length() && (Character.isLetter(s.charAt(i)) || s.charAt(i) == '_')) {
                throw new IllegalArgumentException("malformed number");
            }
        }

        private String operator() {
            for (String op : List.of("<=", ">=", "<>", "!=", "==", "=", "<", ">")) {
                if (s.startsWith(op, i)) {
                    i += op.length();
                    return op;
                }
            }
            return null;
        }

        private boolean peek(char c) {
            return i < s.length() && s.charAt(i) == c;
        }

        private void expect(char c) {
            if (!peek(c)) throw new IllegalArgumentException("expected '" + c + "'");
            i++;
        }

        private void skip() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }
    }

    private static void identifier(String where, String value) {
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_IDENTIFIER) {
            throw new IllegalArgumentException(where + ": identifier exceeds " + MAX_IDENTIFIER + " bytes");
        }
    }

    private static List<String> append(List<String> prefix, String name) {
        List<String> path = new ArrayList<>(prefix.size() + 1);
        path.addAll(prefix);
        path.add(name);
        return List.copyOf(path);
    }
}
