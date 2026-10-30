// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.ClientCapabilities;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The extension's ATTACH-time constraint rules, applied by the worker before it serves a
 * table, and the read-profile negotiation that makes the records readable at all.
 */
public final class ConstraintContract {

    /** The read profiles a catalog serving constraint records advertises. */
    public static final List<String> READ_PROFILES =
            List.of(ConstraintRecord.CONSTRAINT_RECORDS_READ, ConstraintRecord.SCALAR_EXPRESSION_READ);

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
        if (available.isEmpty() || clientCapabilities == null || clientCapabilities.length == 0) return List.of();
        ClientCapabilities client = RecordCodec.deserializeFromBytes(clientCapabilities, ClientCapabilities.class);
        List<String> offered = client.catalog() == null || client.catalog().length == 0 ? List.of()
                : RecordCodec.deserializeFromBytes(client.catalog(), CatalogCapabilities.class).read();
        var selected = new TreeSet<String>();
        for (String p : available) if (offered.contains(p)) selected.add(p);
        if (!selected.contains(ConstraintRecord.CONSTRAINT_RECORDS_READ)) {
            selected.remove(ConstraintRecord.SCALAR_EXPRESSION_READ);
        }
        return List.copyOf(selected);
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
        if (required.isEmpty()) return explicit;
        CatalogCapabilities advertised = decode(explicit.advertised());
        CatalogCapabilities selected = decode(explicit.selected());
        var advRead = new TreeSet<>(advertised.read());
        advRead.addAll(required);
        var selRead = new TreeSet<>(selected.read());
        selRead.addAll(selectReadProfiles(required, clientCapabilities));
        return new Worker.CatalogCapabilitySelection(
                RecordCodec.serializeToBytes(new CatalogCapabilities(List.copyOf(advRead),
                        advertised.ddl(), advertised.write(), advertised.transaction())),
                RecordCodec.serializeToBytes(new CatalogCapabilities(List.copyOf(selRead),
                        selected.ddl(), selected.write(), selected.transaction())));
    }

    private static CatalogCapabilities decode(byte[] bytes) {
        return bytes == null || bytes.length == 0
                ? new CatalogCapabilities(List.of(), List.of(), List.of(), List.of())
                : RecordCodec.deserializeFromBytes(bytes, CatalogCapabilities.class);
    }

    /**
     * Checks a table the way the extension does at ATTACH: every record is a well-formed
     * {@code ENFORCED} + {@code ALL_ROWS} record owned by this table, and Arrow nullability
     * agrees with the {@code NOT_NULL}/{@code PRIMARY_KEY} records (a field, descending
     * through ordinary STRUCTs, is non-nullable exactly when a record names its path).
     * Applies to every table, including one with no records.
     *
     * @param info the table about to be served
     * @throws IllegalArgumentException naming the table and column path on a violation
     */
    public static void validate(TableInfo info) {
        String table = "'" + String.join(".", info.schema_path()) + "." + info.name() + "'";
        Set<List<String>> required = new HashSet<>();
        Set<String> ids = new HashSet<>();
        int index = 0;
        for (byte[] bytes : info.constraints()) {
            ConstraintRecord record;
            try {
                record = ConstraintRecord.decode(bytes);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Table " + table + ": constraints[" + index
                        + "] is not a ConstraintInfo record: " + e.getMessage(), e);
            }
            index++;
            if (!record.isEngineFact()) {
                throw new IllegalArgumentException("Table " + table + ": constraint '" + record.id() + "' is "
                        + record.enforcement() + "/" + record.validation()
                        + "; DuckDB accepts only ENFORCED + ALL_ROWS records");
            }
            if (!record.table().equals(info.name()) || !record.schemaPath().equals(info.schema_path())) {
                throw new IllegalArgumentException("Table " + table + ": constraint '" + record.id()
                        + "' is owned by '" + String.join(".", record.schemaPath()) + "." + record.table() + "'");
            }
            if (!ids.add(record.id())) {
                throw new IllegalArgumentException("Table " + table + ": duplicate constraint id '" + record.id() + "'");
            }
            if (record.kind() == ConstraintRecord.Kind.NOT_NULL || record.kind() == ConstraintRecord.Kind.PRIMARY_KEY) {
                required.addAll(record.columns());
            }
        }
        Schema schema = info.columns() == null || info.columns().length == 0
                ? null : SchemaUtil.deserializeSchema(info.columns());
        Set<List<String>> seen = new HashSet<>();
        if (schema != null) walk(table, schema.getFields(), List.of(), required, seen);
        for (List<String> path : required) {
            if (!seen.contains(path)) {
                throw new IllegalArgumentException("Table " + table + ": a NOT_NULL/PRIMARY_KEY record names column "
                        + String.join(".", path) + ", which is not a column reachable through ordinary STRUCTs");
            }
        }
    }

    private static void walk(String table, List<Field> fields, List<String> prefix,
                             Set<List<String>> required, Set<List<String>> seen) {
        for (Field field : fields) {
            Map<String, String> metadata = field.getMetadata();
            if (prefix.isEmpty() && metadata != null && metadata.containsKey("is_row_id")) continue;
            List<String> path = new ArrayList<>(prefix);
            path.add(field.getName());
            path = List.copyOf(path);
            seen.add(path);
            boolean named = required.contains(path);
            if (field.isNullable() == named) {
                String column = String.join(".", path);
                throw new IllegalArgumentException(field.isNullable()
                        ? "Table " + table + ": column " + column
                                + " is in a NOT_NULL/PRIMARY_KEY record but its Arrow field is nullable"
                        : "Table " + table + ": column " + column
                                + " is non-nullable in Arrow but no NOT_NULL/PRIMARY_KEY record names it;"
                                + " declare one with ConstraintRecord.builder(..) or make the field nullable");
            }
            if (ordinaryStruct(field)) walk(table, field.getChildren(), path, required, seen);
        }
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
}
