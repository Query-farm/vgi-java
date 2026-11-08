# Filtered scan planning (wire 3.2)

Protocol 3.2 lets a catalog table whose scan is redirected to other functions
(`read_parquet`, `iceberg_scan`, a VGI table function, a companion catalog's
table) see the query's predicates before it chooses what to scan, and lets the
client prune the branches it returns. The normative spec is vgi-python's
[`docs/protocol/scan-planning.md`](https://github.com/Query-farm/vgi-python/blob/main/docs/protocol/scan-planning.md).

Every mechanism is pruning only: the client still applies every predicate to the
rows the remaining branches produce, so a planner that ignores the filters, or
returns more than it needs to, stays correct. Omitting a branch that holds a
matching row is a bug.

## Worker side

Register the table with a planner. `ScanPlanning.FILTERED` makes the table
advertise `TableInfo.scan_planning = "filtered"`: the client defers
`catalog_table_scan_branches_get` until it has collected the scan's predicates
and passes them as `pushdown_filters` (a filter-v2 snapshot whose column indexes
refer to the stub's declared columns).

```java
w.registerPlannedTable(
        CatalogTable.builder("data", "events", columns).build(),
        ScanPlanning.FILTERED,
        request -> {
            ScanPlanningFilters filters = request.pushdownFilters(); // null at bind time
            List<ScanBranch> branches = manifest.stream()
                    .filter(file -> filters == null || filters.mayMatch(file.statistics()))
                    .map(file -> ScanBranch.of("read_parquet", file.path())
                            .withStatistics(file.rowCount(), file.statistics()))
                    .toList();
            return new ScanBranchesResult(branches, List.of("parquet"));
        });
```

- `ScanPlanningFilters.decode()` returns the decoded `PushdownFilters`
  (lazily, against the table's declared columns); `raw()` returns the wire bytes.
- `ScanPlanningFilters.mayMatch(statistics)` returns `false` only when some
  predicate is provably false for every row described by per-column
  min/max/`hasNull`/`hasNotNull`. Anything it cannot reason about (casts,
  functions, mismatched types) counts as "may match".
- A filtered table may return zero branches, meaning the scan produces no rows.
  A filtered table never inlines `scan_function`; `TableInfo` rejects the
  combination.
- `ScanBranch.withStatistics(rowCount, columnStatistics)` attaches the 3.2 branch
  statistics. `rowCount` feeds cardinality estimation; `columnStatistics` are
  conservative claims the client prunes by, honoured for bind-time tables too.
  Wrong statistics silently drop rows, so leave a column out rather than guess.

Every other table goes through the worker-wide planner,
`Worker.scanBranchesPlanner(planner)`. Its default,
`ScanBranchesPlanner.DEFAULT`, ignores the predicates and returns
`request.defaultResult()`, the branches the worker served under 3.1 (declared
multi-branch branches, or a one-branch wrap of the table's scan function), so
existing workers need no changes.

## Client side

`TableInfoDecoder` reads `scan_planning` (a pre-3.2 sender decodes as `"bind"`;
an unknown value is an error). `ScanBranchesDecoder` decodes a
`catalog_table_scan_branches_get` result, including `rowCount`,
`columnStatistics` and an empty branch list. A client that cannot defer planning
calls `catalog_table_scan_branches_get` at bind time with
`pushdown_filters = null`, which is always permitted.
