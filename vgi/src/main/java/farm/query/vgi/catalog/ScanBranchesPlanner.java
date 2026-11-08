// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Protocol 3.2 planning hook for {@code catalog_table_scan_branches_get}.
 *
 * <p>The worker calls a planner for every {@code catalog_table_scan_branches_get}.
 * A planner registered for one table with
 * {@code Worker.registerPlannedTable} answers that table; every other table goes
 * through {@code Worker.scanBranchesPlanner}, whose default,
 * {@link #DEFAULT}, ignores the predicates and returns
 * {@link Request#defaultResult()} — the branches the worker would have served
 * under protocol 3.1 (declared multi-branch branches, or a one-branch wrap of
 * the table's scan function). Existing workers therefore behave unchanged.</p>
 *
 * <p>Every predicate is advisory: the client re-applies all of them, so return
 * any superset of the branches and files that could hold matching rows, and
 * never drop one that could. {@link ScanPlanningFilters#mayMatch} prunes
 * candidates by min/max statistics. A {@link ScanPlanning#FILTERED} table may
 * return zero branches, meaning the scan produces no rows. Mirrors vgi-python
 * {@code CatalogInterface.table_scan_branches_plan}; see
 * {@code docs/protocol/scan-planning.md}.</p>
 */
@FunctionalInterface
public interface ScanBranchesPlanner {

    /** Ignores the predicates and serves the 3.1 branches. */
    ScanBranchesPlanner DEFAULT = Request::defaultResult;

    /**
     * Plan a table's scan branches.
     *
     * @param request the table, time-travel point and predicates
     * @return the branches; zero is legal only for a filtered table
     */
    ScanBranchesResult plan(Request request);

    /**
     * One {@code catalog_table_scan_branches_get} call.
     *
     * @param catalogName    the auxiliary catalog the table lives in, or {@code null}
     *                       for this worker's own catalog
     * @param schemaPath     the schema containing the table
     * @param name           the table name
     * @param atUnit         the time-travel unit, or {@code null}
     * @param atValue        the time-travel value, or {@code null}
     * @param pushdownFilters the query's predicates on the table, or {@code null} when
     *                       none were collected (bind-time planning, or no usable predicate)
     * @param defaults       computes the 3.1 answer for this table on demand
     */
    record Request(
            @Nullable String catalogName,
            List<String> schemaPath,
            String name,
            @Nullable String atUnit,
            @Nullable String atValue,
            @Nullable ScanPlanningFilters pushdownFilters,
            Supplier<ScanBranchesResult> defaults) {

        /** Validates and copies the schema path. */
        public Request {
            schemaPath = List.copyOf(Objects.requireNonNull(schemaPath, "schemaPath"));
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(defaults, "defaults");
        }

        /**
         * The branches the worker serves for this table without planning:
         * declared multi-branch branches, or a one-branch wrap of its scan function.
         *
         * @return the 3.1 answer
         * @throws farm.query.vgi.NotFoundException if the table has no scan to wrap
         */
        public ScanBranchesResult defaultResult() {
            return defaults.get();
        }
    }
}
