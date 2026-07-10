// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-attach catalog state and version resolution.
 *
 * <p>Holds, keyed by {@code attach_id}:
 * <ul>
 *   <li>The resolved {@code data_version} string negotiated at ATTACH time,
 *       used to default the version for queries that don't pass an AT clause
 *       (information_schema, plain {@code SELECT * FROM ...}).</li>
 *   <li>The catalog name from the {@code ATTACH 'name'} clause, used to
 *       filter fixture-vs-fixture across multiple logical catalogs served
 *       by the same worker binary.</li>
 * </ul>
 *
 * <p>Also owns {@link #resolveVersion}: for time-travel-capable tables,
 * swap the declared columns + scan args for a version-specific variant
 * registered under {@code <name>_v<N>} or {@code <name>_v_<X>_<Y>_<Z>}.
 */
public final class CatalogRegistry {

    /**
     * Per-attach record: the resolved data_version + catalog name (either may be null).
     *
     * @param dataVersion the data version negotiated at ATTACH time, or {@code null}
     * @param catalogName the catalog name from the {@code ATTACH 'name'} clause, or {@code null}
     */
    public record AttachRecord(String dataVersion, String catalogName) {}

    private final Worker worker;
    private final Map<String, AttachRecord> attaches = new ConcurrentHashMap<>();

    /**
     * Creates a registry bound to its owning worker.
     *
     * @param worker the owning worker (supplies catalog name and registered tables)
     */
    public CatalogRegistry(Worker worker) {
        this.worker = worker;
    }

    /**
     * Record (or replace) the per-attach state. Called once per {@code
     * catalog_attach} RPC. If both fields are null/empty the call is a
     * no-op rather than inserting an empty record.
     *
     * @param attachId            the attach identifier (Map key)
     * @param resolvedDataVersion the negotiated data version, or {@code null}
     * @param catalogName         the ATTACH catalog name, or {@code null}/empty
     */
    public void recordAttach(byte[] attachId, String resolvedDataVersion, String catalogName) {
        String name = (catalogName == null || catalogName.isEmpty()) ? null : catalogName;
        if (resolvedDataVersion == null && name == null) return;
        attaches.put(HexId.encode(attachId), new AttachRecord(resolvedDataVersion, name));
    }

    /**
     * The data version negotiated at ATTACH time for this attach.
     *
     * @param attachId the attach identifier
     * @return the resolved data version for this attach, or {@code null} if unknown
     */
    public String dataVersion(byte[] attachId) {
        AttachRecord r = lookup(attachId);
        return r == null ? null : r.dataVersion();
    }

    /**
     * The catalog name from the {@code ATTACH 'name'} clause for this attach.
     *
     * @param attachId the attach identifier
     * @return the ATTACH catalog name for this attach, or {@code null} if unknown
     */
    public String catalogName(byte[] attachId) {
        AttachRecord r = lookup(attachId);
        return r == null ? null : r.catalogName();
    }

    private AttachRecord lookup(byte[] attachId) {
        return attachId == null ? null : attaches.get(HexId.encode(attachId));
    }

    /**
     * If the caller didn't pass an AT clause, default to the resolved
     * data version recorded for this attach. Otherwise return the caller's
     * clause unchanged.
     *
     * @param attachId the attach identifier
     * @param atUnit   the caller's AT clause unit, or {@code null}/empty if absent
     * @param atValue  the caller's AT clause value
     * @return the effective AT clause to apply
     */
    public AtClause effectiveAt(byte[] attachId, String atUnit, String atValue) {
        if (atUnit != null && !atUnit.isEmpty()) return new AtClause(atUnit, atValue);
        String dv = dataVersion(attachId);
        return dv == null ? new AtClause(atUnit, atValue) : new AtClause("data_version", dv);
    }

    /**
     * A resolved time-travel AT clause.
     *
     * @param unit  the AT unit (e.g. {@code version}, {@code timestamp}, {@code data_version})
     * @param value the AT value
     */
    public record AtClause(String unit, String value) {}

    /**
     * For the {@code versioned_tables} catalog only, gate the plants/animals
     * fixtures by the attach's resolved data version. Returns {@code true} if
     * the table should be hidden from the catalog.
     *
     * @param tableName the candidate table name
     * @param attachId  the attach identifier (supplies the resolved data version)
     * @return {@code true} if the table should be hidden at the attach's data version
     */
    public boolean isHiddenInVersionedTables(String tableName, byte[] attachId) {
        if (!"versioned_tables".equals(worker.catalogName())) return false;
        String dv = dataVersion(attachId);
        if (dv == null) return false;
        if ("plants".equals(tableName) && SemverHelpers.compareVersions(dv, "2.0.0") < 0) return true;
        if ("animals".equals(tableName) && SemverHelpers.compareVersions(dv, "3.0.0") >= 0) return true;
        return false;
    }

    /**
     * For {@code versioned_data} / {@code versioned_constraints}, swap the
     * declared columns + scan args for a version-specific variant when the
     * client asked {@code AT (VERSION => N)}. Other tables and AT clauses
     * pass through unchanged. Throws if N is out of range so DuckDB surfaces
     * "Unknown version" / "table did not exist before {@code <year>}" cleanly.
     *
     * @param t        the base table being bound
     * @param at_unit  the AT clause unit, or {@code null}/empty for no time travel
     * @param at_value the AT clause value
     * @return the version-specific table variant, or {@code t} unchanged when no swap applies
     * @throws IllegalArgumentException if the requested version is out of range or unsupported
     */
    public CatalogTable resolveVersion(CatalogTable t, String at_unit, String at_value) {
        if (at_unit == null || at_value == null || at_unit.isEmpty()) return t;
        if ("data_version".equalsIgnoreCase(at_unit)) {
            String suffix = "_v_" + at_value.replace('.', '_');
            String dvName = t.name() + suffix;
            for (CatalogTable vt : worker.catalogTables()) {
                if (vt.schema().equals(t.schema()) && vt.name().equals(dvName)) {
                    return new CatalogTable(
                            t.schema(), t.name(), vt.columns(), t.comment(), t.tags(),
                            vt.scanFunctionName(), vt.scanFunctionPositional(), vt.scanFunctionNamed(),
                            null, null, false, true,
                            List.of(), List.of(), List.of(), List.of());
                }
            }
            return t;
        }
        if (!"versioned_data".equals(t.name()) && !"versioned_constraints".equals(t.name())) {
            // Tables that resolve AT themselves rather than via a name-swapped
            // variant: tt_pushdown_fn is function-backed (the function reads AT
            // at init via the bind request); tt_pushdown_cols is columns-based
            // (catalog_table_scan_function_get bakes the resolved version into a
            // scan-function argument). Pass them through unchanged instead of
            // rejecting the AT clause.
            if ("tt_pushdown_fn".equals(t.name()) || "tt_pushdown_cols".equals(t.name())
                    || "cache_versioned".equals(t.name())) return t;
            throw new IllegalArgumentException("table " + t.name() + " does not support time travel");
        }
        int version = -1;
        if ("version".equalsIgnoreCase(at_unit)) {
            try { version = Integer.parseInt(at_value); }
            catch (NumberFormatException e) { throw new IllegalArgumentException("Unknown version: " + at_value); }
        } else if ("timestamp".equalsIgnoreCase(at_unit)) {
            String s = at_value;
            int yearEnd = Math.min(4, s.length());
            int year;
            try { year = Integer.parseInt(s.substring(0, yearEnd)); }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Unknown timestamp: " + at_value);
            }
            if (year < 2020) {
                throw new IllegalArgumentException("table did not exist before 2020");
            }
            if (year < 2021) version = 1;
            else if (year < 2022) version = 2;
            else version = 3;
        } else {
            return t;
        }
        if (version < 1 || version > 3) {
            throw new IllegalArgumentException("Unknown version: " + version);
        }
        String versionedName = t.name() + "_v" + version;
        for (CatalogTable vt : worker.catalogTables()) {
            if (vt.schema().equals(t.schema()) && vt.name().equals(versionedName)) {
                return new CatalogTable(
                        t.schema(), t.name(), vt.columns(), t.comment(), t.tags(),
                        vt.scanFunctionName(), vt.scanFunctionPositional(), vt.scanFunctionNamed(),
                        null, null, false, true,
                        List.of(), List.of(), List.of(), List.of());
            }
        }
        return t;
    }
}
