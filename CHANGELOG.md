# Changelog

## Unreleased

### Changed

- **Protocol 3.3.0.** `VgiService.PROTOCOL_VERSION` is now `3.3.0`; peers must match major and
  minor, so this release needs the matching VGI extension (protocol 3.3).
- **Table scan and write functions are hidden from SQL.** Every function a table descriptor
  names (scan, insert, update, delete) is now always sent in `catalog_schema_contents_functions`,
  carrying the new `FunctionInfo.visibility` field. Write functions are always `table_only`; a
  table's scan function is `table_only` unless the table sets `exposeScan`. `table_only`
  functions no longer appear in `duckdb_functions()` and can no longer be called directly
  (e.g. `SELECT * FROM cat.main.items_scan()` now fails); the tables they back work as before,
  with their capabilities (projection / filter pushdown, required secrets, `has_finalize`).
- `Worker.registerUnlistedTable` keeps working but now lists the function as `table_only`
  instead of omitting it, so the extension learns its capabilities. It is superseded for table
  scans, which are `table_only` by default.

### Added

- `CatalogTable.Builder.exposeScan(boolean)` and the `CatalogTable.exposeScan()` component
  (default `false`): make a table's scan function an ordinary, SQL-callable catalog function.
- `FunctionInfo.visibility` with `FunctionInfo.VISIBILITY_PUBLIC` / `VISIBILITY_TABLE_ONLY`
  and `FunctionInfo.withVisibility(String)`. The pre-3.3 `FunctionInfo` and `CatalogTable`
  constructor shapes remain as compatibility constructors.
