// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ColumnStatistics;
import farm.query.vgi.catalog.ScanBranch;
import farm.query.vgi.catalog.ScanBranchesPlanner;
import farm.query.vgi.catalog.ScanBranchesResult;
import farm.query.vgi.catalog.ScanPlanning;
import farm.query.vgi.catalog.ScanPlanningFilters;
import farm.query.vgi.client.FilterPredicate;
import farm.query.vgi.client.ProjectedColumn;
import farm.query.vgi.client.PushdownFiltersEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.client.ScanBranchesDecoder;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static farm.query.vgi.internal.CatalogContractsTest.fixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Protocol 3.2 filtered scan planning (vgi-python docs/protocol/scan-planning.md). */
final class ScanPlanningTest {

    private static final ArrowType DATE = new ArrowType.Date(DateUnit.DAY);
    private static final Schema EVENTS = Schemas.of(
            Schemas.nullable("event_date", DATE), Schemas.nullable("n", Schemas.INT64));
    private static final long OCT_3 = LocalDate.of(2026, 10, 3).toEpochDay();

    private static ColumnStatistics dateStats(LocalDate lo, LocalDate hi) {
        return new ColumnStatistics("event_date", DATE, lo, hi, false, true, null, null, null);
    }

    // ------------------------------------------------------------------ conformance

    @Test
    void pythonBranchCarriesRowCountAndColumnStatistics() throws Exception {
        ScanBranchesDecoder.Branch branch = ScanBranchesDecoder.decodeBranch(fixture("scan_planning_branch"));
        assertEquals("read_parquet", branch.functionName());
        assertEquals(1000L, branch.rowCount());
        assertEquals(2, branch.columnStatistics().size());
        ColumnStatistics date = branch.columnStatistics().get(0);
        assertEquals("event_date", date.columnName());
        assertEquals(DATE, date.arrowType());
        assertEquals(OCT_3, ((Number) date.min()).longValue());
        assertEquals(OCT_3, ((Number) date.max()).longValue());
        assertFalse(date.hasNull());
        ColumnStatistics n = branch.columnStatistics().get(1);
        assertEquals(0L, n.min());
        assertEquals(999L, n.max());
        assertTrue(n.hasNull());
        assertEquals(true, branch.decodedArguments().named().get("hive_partitioning").value());
    }

    @Test
    void pythonBranchesResultsDecodeIncludingZeroBranches() throws Exception {
        ScanBranchesDecoder.Result result = ScanBranchesDecoder.decode(fixture("scan_planning_branches_result"));
        assertEquals(1, result.branches().size());
        assertEquals(List.of("parquet"), result.requiredExtensions());
        assertEquals(1000L, result.branches().getFirst().rowCount());

        ScanBranchesDecoder.Result empty = ScanBranchesDecoder.decode(fixture("scan_planning_empty_branches_result"));
        assertTrue(empty.branches().isEmpty());
        assertTrue(empty.requiredExtensions().isEmpty());
    }

    @Test
    void pythonFilteredTableInfoRoundTrips() throws Exception {
        TableInfo info = TableInfoDecoder.decode(fixture("scan_planning_table_info"));
        assertEquals("filtered", info.scan_planning());
        assertTrue(info.scan_function() == null || info.scan_function().length == 0);
        TableInfo again = TableInfoDecoder.decode(TableInfoSerializer.encode(info));
        assertEquals("filtered", again.scan_planning());
        assertEquals("bind", TableInfoDecoder.decode(fixture("table_info")).scan_planning());
    }

    @Test
    void pythonPushdownFiltersDecodeAndPruneByStatistics() throws Exception {
        ScanPlanningFilters filters = ScanPlanningFilters.of(fixture("scan_planning_pushdown_filters"), EVENTS);
        assertEquals(2, filters.decode().predicates().size());
        // WHERE event_date >= DATE '2026-10-03' AND n < 100
        List<ColumnStatistics> branch = ScanBranchesDecoder.decodeBranch(fixture("scan_planning_branch"))
                .columnStatistics();
        assertTrue(filters.mayMatch(branch));
        assertFalse(filters.mayMatch(List.of(dateStats(LocalDate.of(2026, 10, 1), LocalDate.of(2026, 10, 2)))));
        assertTrue(filters.mayMatch(List.of(dateStats(LocalDate.of(2026, 10, 2), LocalDate.of(2026, 10, 3)))));
        assertFalse(filters.mayMatch(List.of(ColumnStatistics.ofInt64("n", 100, 500, false, null))));
        assertTrue(filters.mayMatch(List.of(ColumnStatistics.ofInt64("n", 99, 500, false, null))));
        assertTrue(filters.mayMatch(List.of(ColumnStatistics.ofInt64("other", 0, 0, false, null))));
        assertTrue(filters.mayMatch(List.of()));
    }

    // ------------------------------------------------------------------ records

    @Test
    void tableInfoValidatesScanPlanning() {
        TableInfo bind = new TableInfo(null, Map.of(), "t", List.of("main"), new byte[0], List.of(), null,
                Map.of(), false, new byte[] {1}, null, null, null, null, null, null, null, List.of());
        assertEquals("bind", bind.scan_planning());
        assertThrows(IllegalArgumentException.class, () -> bind.withScanPlanning("filtered"));
        assertThrows(IllegalArgumentException.class, () -> bind.withScanPlanning("lazy"));
        TableInfo noScan = new TableInfo(null, Map.of(), "t", List.of("main"), new byte[0], List.of(), null,
                Map.of(), false, null, null, null, null, null, null, null, null, List.of());
        assertEquals("filtered", noScan.withScanPlanning("filtered").scan_planning());
        assertEquals(ScanPlanning.FILTERED, ScanPlanning.fromWire("filtered"));
        assertThrows(IllegalArgumentException.class, () -> ScanPlanning.fromWire("Filtered"));
    }

    @Test
    void scanBranchValidatesStatistics() {
        ScanBranch b = ScanBranch.of("sequence", 5L);
        assertThrows(IllegalArgumentException.class, () -> b.withStatistics(-1L, null));
        assertThrows(IllegalArgumentException.class, () -> b.withStatistics(1L, List.of(
                ColumnStatistics.ofInt64("n", 0, 1, false, null),
                ColumnStatistics.ofInt64("n", 2, 3, false, null))));
        ScanBranch withStats = b.withStatistics(5L, List.of(ColumnStatistics.ofInt64("n", 0, 4, false, null)));
        assertEquals(5L, withStats.withSchemaPath(List.of("main")).rowCount());
        assertEquals(1, withStats.withSchemaPath(List.of("main")).columnStatistics().size());
    }

    @Test
    void javaBranchStatisticsRoundTripThroughTheClientDecoder() {
        ScanBranch branch = ScanBranch.of("read_parquet", "s3://b/day.parquet")
                .withStatistics(1000L, List.of(dateStats(LocalDate.of(2026, 10, 3), LocalDate.of(2026, 10, 3)),
                        ColumnStatistics.ofInt64("n", 0, 999, true, null)));
        byte[] bytes = ScanBranchesResultSerializer.serialize(
                new ScanBranchesResult(List.of(branch, ScanBranch.of("sequence", 3L)), List.of("parquet")));
        ScanBranchesDecoder.Result result = ScanBranchesDecoder.decode(bytes);
        assertEquals(List.of("parquet"), result.requiredExtensions());
        ScanBranchesDecoder.Branch first = result.branches().get(0);
        assertEquals(1000L, first.rowCount());
        assertEquals(OCT_3, ((Number) first.columnStatistics().get(0).min()).longValue());
        assertEquals(999L, first.columnStatistics().get(1).max());
        ScanBranchesDecoder.Branch second = result.branches().get(1);
        assertNull(second.rowCount());
        assertNull(second.columnStatistics());

        assertTrue(ScanBranchesDecoder.decode(ScanBranchesResultSerializer.serialize(ScanBranchesResult.empty()))
                .branches().isEmpty());
    }

    // ------------------------------------------------------------------ mayMatch

    private static ScanPlanningFilters filters(String column, int index, FilterPredicate p) {
        byte[] raw = PushdownFiltersEncoder.builder().filter(ProjectedColumn.of(column, index), p).encode()
                .pushdownFilters();
        return ScanPlanningFilters.of(raw, EVENTS);
    }

    private static ScanPlanningFilters nFilter(FilterPredicate p) {
        return filters("n", 1, p);
    }

    @Test
    void mayMatchReasonsAboutComparisonsInAndNulls() {
        List<ColumnStatistics> tenToTwenty = List.of(ColumnStatistics.ofInt64("n", 10, 20, false, null));
        assertFalse(nFilter(FilterPredicate.eq(5L)).mayMatch(tenToTwenty));
        assertTrue(nFilter(FilterPredicate.eq(15L)).mayMatch(tenToTwenty));
        assertFalse(nFilter(FilterPredicate.gt(20L)).mayMatch(tenToTwenty));
        assertTrue(nFilter(FilterPredicate.ge(20L)).mayMatch(tenToTwenty));
        assertFalse(nFilter(FilterPredicate.lt(10L)).mayMatch(tenToTwenty));
        assertTrue(nFilter(FilterPredicate.le(10L)).mayMatch(tenToTwenty));
        assertTrue(nFilter(FilterPredicate.ne(15L)).mayMatch(tenToTwenty));
        assertFalse(nFilter(FilterPredicate.ne(7L)).mayMatch(
                List.of(ColumnStatistics.ofInt64("n", 7, 7, true, null))));
        // Integer literal against an int64 column compares by value.
        assertFalse(nFilter(FilterPredicate.compare(farm.query.vgi.pushdown.ComparisonOperator.GT,
                ScalarValue.of(ScalarValue.INT32, 30))).mayMatch(tenToTwenty));
        // OR excludes only when every arm does.
        assertFalse(nFilter(FilterPredicate.or(FilterPredicate.lt(5L), FilterPredicate.gt(25L)))
                .mayMatch(tenToTwenty));
        assertTrue(nFilter(FilterPredicate.or(FilterPredicate.lt(5L), FilterPredicate.gt(15L)))
                .mayMatch(tenToTwenty));
        assertFalse(nFilter(FilterPredicate.and(FilterPredicate.gt(0L), FilterPredicate.lt(10L)))
                .mayMatch(tenToTwenty));
        // NULL claims.
        assertFalse(nFilter(FilterPredicate.isNull()).mayMatch(tenToTwenty));
        List<ColumnStatistics> allNull = List.of(new ColumnStatistics("n", new ArrowType.Null(), null, null,
                true, false, null, null, null));
        assertFalse(nFilter(FilterPredicate.isNotNull()).mayMatch(allNull));
        assertFalse(nFilter(FilterPredicate.eq(1L)).mayMatch(allNull));
        assertTrue(nFilter(FilterPredicate.isNull()).mayMatch(allNull));
        // Unknown bounds keep the candidate.
        List<ColumnStatistics> unbounded = List.of(new ColumnStatistics("n", new ArrowType.Null(), null, null,
                false, true, null, null, null));
        assertTrue(nFilter(FilterPredicate.eq(1L)).mayMatch(unbounded));
    }

    @Test
    void mayMatchKeepsCandidatesItCannotCompare() {
        // Statistics of another type than the literal: not comparable, so kept.
        assertTrue(nFilter(FilterPredicate.eq(15L)).mayMatch(List.of(
                ColumnStatistics.ofUtf8("n", "a", "b", false, null, null, null))));
        // Date literal on the date column, statistics given as LocalDate.
        ScanPlanningFilters onDate = filters("event_date", 0,
                FilterPredicate.compare(farm.query.vgi.pushdown.ComparisonOperator.LT,
                        ScalarValue.of(DATE, (int) OCT_3)));
        assertFalse(onDate.mayMatch(List.of(dateStats(LocalDate.of(2026, 10, 3), LocalDate.of(2026, 10, 5)))));
        assertTrue(onDate.mayMatch(List.of(dateStats(LocalDate.of(2026, 10, 2), LocalDate.of(2026, 10, 5)))));
    }

    // ------------------------------------------------------------------ service dispatch

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    @Test
    void filteredTableAdvertisesModeAndPlannerReceivesDecodedFilters() throws Exception {
        AtomicReference<ScanBranchesPlanner.Request> seen = new AtomicReference<>();
        List<LocalDate> days = new ArrayList<>();
        for (int d = 1; d <= 5; d++) days.add(LocalDate.of(2026, 10, d));
        Worker w = Worker.builder().catalogName("cat").registerPlannedTable(
                CatalogTable.builder("main", "events", SchemaUtil.serializeSchema(EVENTS)).build(),
                ScanPlanning.FILTERED,
                request -> {
                    seen.set(request);
                    var f = request.pushdownFilters();
                    return new ScanBranchesResult(days.stream()
                            .filter(day -> f == null || f.mayMatch(List.of(dateStats(day, day))))
                            .map(day -> ScanBranch.of("read_parquet", day + ".parquet")
                                    .withStatistics(10L, List.of(dateStats(day, day))))
                            .toList(), List.of("parquet"));
                });
        VgiServiceImpl svc = service(w);
        byte[] attach = svc.catalog_attach(new CatalogAttachRequest("cat", null, null, null, null), null)
                .attach_opaque_data();

        TableInfo info = TableInfoDecoder.decode(svc.catalog_table_get(attach, List.of("main"), "events",
                null, null, null, null).items().getFirst());
        assertEquals("filtered", info.scan_planning());
        assertTrue(info.scan_function() == null || info.scan_function().length == 0);

        // Bind-time call: no filters, every day.
        var all = ScanBranchesDecoder.decode(svc.catalog_table_scan_branches_get(
                attach, List.of("main"), "events", null, null, null, null, null));
        assertEquals(5, all.branches().size());
        assertNull(seen.get().pushdownFilters());

        // event_date >= 2026-10-03 AND n < 100 → three days.
        var pruned = ScanBranchesDecoder.decode(svc.catalog_table_scan_branches_get(
                attach, List.of("main"), "events", null, null, null,
                fixture("scan_planning_pushdown_filters"), null));
        assertEquals(3, pruned.branches().size());
        assertEquals(10L, pruned.branches().getFirst().rowCount());
        assertNotNull(seen.get().pushdownFilters());
        assertEquals(List.of("main"), seen.get().schemaPath());

        // Nothing matches → zero branches, which is legal for a filtered table.
        byte[] none = PushdownFiltersEncoder.builder()
                .filter(ProjectedColumn.of("event_date", 0), FilterPredicate.compare(
                        farm.query.vgi.pushdown.ComparisonOperator.GT,
                        ScalarValue.of(DATE, (int) LocalDate.of(2027, 1, 1).toEpochDay())))
                .encode().pushdownFilters();
        var empty = ScanBranchesDecoder.decode(svc.catalog_table_scan_branches_get(
                attach, List.of("main"), "events", null, null, null, none, null));
        assertTrue(empty.branches().isEmpty());
        assertEquals(List.of("parquet"), empty.requiredExtensions());
    }

    @Test
    void defaultPlannerIgnoresFiltersAndServesThe31Branches() throws Exception {
        AtomicReference<ScanBranchesPlanner.Request> seen = new AtomicReference<>();
        Worker w = Worker.builder().catalogName("cat")
                .registerMultiBranchTable(
                        CatalogTable.builder("main", "multi", SchemaUtil.serializeSchema(EVENTS)).build(),
                        List.of(ScanBranch.of("sequence", 1L), ScanBranch.of("sequence", 2L)))
                .scanBranchesPlanner(request -> {
                    seen.set(request);
                    return ScanBranchesPlanner.DEFAULT.plan(request);
                });
        VgiServiceImpl svc = service(w);
        byte[] attach = svc.catalog_attach(new CatalogAttachRequest("cat", null, null, null, null), null)
                .attach_opaque_data();
        TableInfo info = TableInfoDecoder.decode(svc.catalog_table_get(attach, List.of("main"), "multi",
                null, null, null, null).items().getFirst());
        assertEquals("bind", info.scan_planning());
        var result = ScanBranchesDecoder.decode(svc.catalog_table_scan_branches_get(
                attach, List.of("main"), "multi", null, null, null,
                fixture("scan_planning_pushdown_filters"), null));
        assertEquals(2, result.branches().size());
        assertNotNull(seen.get().pushdownFilters());
        assertEquals(2, seen.get().pushdownFilters().decode().predicates().size());
    }
}
