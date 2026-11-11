# Changelog

## Unreleased

### Changed

- **Protocol 3.3 write path** (protocol version stays `3.3.0`; needs the matching VGI extension):
  - `catalog_table_scan_function_get` is removed from `VgiService` and from the worker;
    `catalog_table_scan_branches_get` is the only scan-discovery RPC (a single-source table
    answers with one branch, as before). A Java client that called
    `catalog_table_scan_function_get` reads the branch through `ScanBranchesDecoder` instead.
    `TableScanFunctionGetResponse` stays as the `ScanFunctionResult` record.
  - `catalog_table_create` now returns `TableCreateResponse(created)` (generated record).
    `CatalogInterface.tableCreateReportingCreated(attachId, request)` supplies it: `false` when
    `IF NOT EXISTS` found an existing table, so `CREATE TABLE IF NOT EXISTS ... AS SELECT` no
    longer appends into it. Its default keeps catalogs that override only `tableCreate`
    working; `InMemoryCatalog` implements it atomically.
  - `write_options` carries only `result_mode`. `WriteOptions` is now
    `record WriteOptions(WriteResultMode resultMode)`; `onConflict()` / `onConflictColumns()`
    and the three-argument constructor remain, deprecated, returning `"throw"` / empty
    (DuckDB rewrites every `INSERT ... ON CONFLICT` into a MERGE before a catalog sees it, so
    those values were never anything else).

- **Protocol 3.3.0.** `VgiService.PROTOCOL_VERSION` is now `3.3.0`; peers must match major and
  minor, so this release needs the matching VGI extension (protocol 3.3).
- **Table scan and write functions are hidden from SQL.** Every function a table descriptor
  names (scan, insert, update, delete) is now always sent in `catalog_schema_contents_functions`,
  carrying the new `FunctionInfo.visibility` field. Write functions are always `table_only`; a
  table's scan function is `table_only` unless the table sets `exposeScan`. `table_only`
  functions no longer appear in `duckdb_functions()` and can no longer be called directly
  (e.g. `SELECT * FROM cat.main.items_scan()` now fails); the tables they back work as before,
  with their capabilities (projection / filter pushdown, required secrets, `has_finalize`).
  **This includes functions you registered as ordinary public functions:** every function
  registered with `registerTable` (or `registerTableInOut`) that some `CatalogTable` names as its
  scan function becomes hidden (`table_only`) unless that table sets `exposeScan(true)`. If SQL
  calls such a scan directly, set `exposeScan(true)` on the table.
- `Worker.registerUnlistedTable` keeps working but now lists the function as `table_only`
  instead of omitting it, so the extension learns its capabilities. It is superseded for table
  scans, which are `table_only` by default.

### Added

- `CatalogInterface.functions(attachId, path)` and the static
  `CatalogInterface.functionInfoFor(fn, schemaPath, visibility)`: a code-implemented catalog
  (`Worker.registerCatalog`) lists the functions its tables name, with protocol 3.3 visibility,
  built exactly as the worker's own listing builds them (the counterpart of vgi-typescript's
  `functionInfoFor`). Functions registered into that catalog with
  `Worker.registerExtraCatalog{TableFunction,TableInOut,Scalar,Aggregate}(catalog.name(), ...)`
  bind on its attaches. Default: no functions, as before.
- `CatalogColumns.withDefault(field, sql, schemaPath)`: declare a column default (canonical
  `vgi.column.default.v1` metadata).
- txfix fixture: `items` gains `items_insert` / `items_delete` (all three write functions with
  `has_finalize`), `qty BIGINT DEFAULT 0`, staged inserts / deletes on the transactional attach,
  and a `writes()` function (one row per write call), per vgi `docs/writable_tx_fixtures.md`.
- The example worker's `return_secret_value` declares `vgi_example` in `requiredSecrets()`, so
  `vgi_secret_types()` lists it in `required_by`.

- `CatalogTable.Builder.exposeScan(boolean)` and the `CatalogTable.exposeScan()` component
  (default `false`): make a table's scan function an ordinary, SQL-callable catalog function.
- `FunctionInfo.visibility` with `FunctionInfo.VISIBILITY_PUBLIC` / `VISIBILITY_TABLE_ONLY`
  and `FunctionInfo.withVisibility(String)`. The pre-3.3 `FunctionInfo` and `CatalogTable`
  constructor shapes remain as compatibility constructors.
