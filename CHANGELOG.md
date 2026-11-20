# Changelog

## Unreleased

### Changed

- **Breaking: VGI wire 3.4.0.** `VgiService.PROTOCOL_VERSION` is `3.4.0`; a worker refuses 3.3 clients and a 3.3
  extension refuses this worker (exact major+minor match). Nothing else in `vgi.v3` changes. 3.4 pairs with
  vgi-rpc's upload-URL protocol v2, which the HTTP entry points now serve through vgi-rpc-java:
  `VGI-Upload-URL-Support: 2`; `__upload_url__/init` takes one `(size, sha256)` entry per upload and answers
  with sized vendor PUT URLs (the vendor's `POST /v1/blobs/put`, purpose `rpc`) plus the `upload_headers` the
  PUT must carry, so storage refuses a body other than the one announced; and an inbound request pointer must
  carry `vgi_rpc.location.sha256`, which is verified before the batch is decoded.

### Added

- **Blob-vendor externalization** (wire-compatible with vgi-python 0.54; contract: the vgi extension's
  `docs/testing/blob-test-catalog.md`). Needs vgi-rpc-java's `farm.query.vgirpc.vendor` (built from source
  until the next vgi-rpc-java release).
  - `ExternalizingStorage` wraps any `FunctionStorage`: every value is enveloped (`0x00||raw`, or the
    `0x01||u64be size||sha256||object_id` pointer to a vendor blob, byte-identical to vgi-python and checked
    against `test/vectors/storage_envelope_v1.json`); values of at least `VGI_STORAGE_EXTERNALIZE_BYTES`
    (default 512 KiB) go to the blob vendor (`VGI_BLOB_VENDOR_URL` + `VGI_BLOB_VENDOR_TOKEN`, purpose `fs`)
    under the `q.<hex>` / `s.<hex>` scopes, verified by size and SHA-256 on read, and cleaned up in the
    background. A blob that cannot be read fails with `storage blob <id> failed its integrity check: sha256
    mismatch` / `... is missing: not found in the blob vendor ...` (`StorageBlobIntegrityException`,
    `StorageBlobMissingException`). `StorageResolver` always wraps `cloudflare-do` (with its 1 MiB per-value
    cap) and wraps the other backends with `VGI_STORAGE_EXTERNALIZE=1`. **Note:** the `cloudflare-do` stored
    format therefore changes to the envelope; finish running queries before upgrading.
  - The HTTP entry points (`Worker.runFromArgs` `--http`, `Worker.runHttp`) read `VGI_BLOB_VENDOR_URL`:
    results of at least `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` (default 1 MiB) are uploaded to the vendor
    (purpose `rpc`, pointer carries `vgi_rpc.location.sha256`), `__upload_url__/init` vends vendor upload URLs
    (`VGI_RPC_MAX_UPLOAD_BYTES` advertised), and inbound pointers are fetched only from the vendor's hosts.
    `VGI_RPC_MAX_REQUEST_BYTES` is advertised and enforced, vendor or not.
  - The example worker serves the `blob_test` catalog (seven functions), and the integration workflow has a
    `vendor` lane: the whole HTTP suite against vgi-rpc's fake blob vendor, then `test/sql/external_storage/*`.

## 0.49.0 (2026-11-19)

### Security

- **Breaking: aggregate state no longer defaults to Java serialization.** `AggregateFunction.serializeState` /
  `deserializeState` used `ObjectOutputStream` / `ObjectInputStream`, and the bytes are read back from shared
  storage (a SQLite file, or a Cloudflare Durable Object over the network) on every update, combine and
  finalize: anyone able to write that storage could run code in the worker. The defaults now throw, and
  `Worker.registerAggregate*` refuses an aggregate that does not override both. **Migration:** write the state's
  fields explicitly; `farm.query.vgi.aggregate.StateCodec` is a typed, bounds-checked codec for it:

  ```java
  @Override public byte[] serializeState(State s) {
      return StateCodec.encode(out -> { out.writeLong(s.total); out.writeLong(s.count); });
  }
  @Override public State deserializeState(byte[] bytes) {
      return StateCodec.decode(bytes, in -> {
          State s = new State(); s.total = in.readLong(); s.count = in.readLong(); return s;
      });
  }
  ```

  `Output`/`Input` also carry strings, byte arrays, string/double/long lists and tagged nested values
  (`writeValue`). State still in storage from a previous release is not readable by the new codec; executions
  are short-lived, so finish or cancel running queries before upgrading. All example aggregates are migrated.
- **Framework payloads in storage are sealed** (sealed framework payload v1, the same format in every VGI SDK:
  HKDF-SHA256 of the signing key, info `vgi.storage.payload.v1`, AES-256-GCM, AAD binding shard, execution,
  namespace and key). The buffered-function init (it carries the resolved secrets) and aggregate constant
  arguments were stored in plaintext. The key material is `VGI_SIGNING_KEY` / `Worker.signingKey` exactly as
  configured, else the per-user host-local key file secret ATTACH options already use
  (`VGI_ATTACH_SECRET_KEY_FILE`). A payload that fails to open is an error, never plaintext. Data a function
  writes itself (buffering appends, aggregate group states, queues) is stored as written.
- **The buffered-function init no longer stores the attach**, and `table_buffering_process` / `_combine` use the
  request's authenticated attach instead of preferring the stored plaintext. A process or combine for an
  execution that never ran init (or after its destructor) now fails with "unknown execution_id" instead of
  running with empty arguments.

### Fixed

- **Aggregates work on the `cloudflare-do` storage tier.** Aggregate state went to the raw backend and sent
  `shard_key: ""`, which the Durable Object rejects; every aggregate RPC is now sharded per attach like buffering.
- The Durable Object client has connect (10 s) and request (30 s) timeouts and retries a transport error,
  5xx or 429 (with backoff and jitter, `Retry-After` honoured) up to 4 attempts, sending the same `attempt_id` on
  every retry of a call so the server replays rather than reapplies it. Other 4xx are not retried. Tune with
  `CfdoStorage.Options` or `VGI_CF_DO_CONNECT_TIMEOUT_MS`, `VGI_CF_DO_REQUEST_TIMEOUT_MS`,
  `VGI_CF_DO_MAX_ATTEMPTS`, `VGI_CF_DO_RETRY_BASE_MS`, `VGI_CF_DO_RETRY_MAX_MS`.

### Changed

- Fewer storage round trips: the buffered-function init and aggregate constant arguments are cached per
  `(shard, execution)` in a bounded process-local LRU (evicted on destructor; storage stays authoritative). A
  buffered process is 1 call per batch (was 2, plus 1 per combine) and an aggregate update 2 (was 3).
- The example worker's buffering drain reads its log 16 entries per storage call instead of 1.

## 0.48.1 (2026-11-16)

### Fixed

- A schema with no declared comment is sent with comment NULL, not `""`. DuckDB counts `""` as a comment, so an
  uncommented schema showed up under `comment IS NOT NULL`.

### Added

- The example worker serves the `contract` schema for the vgi extension's cross-SDK conformance files (no
  `amounts` table: arrow-java has no Decimal32/Decimal64, so its lane excludes `example_decimal_width.test`).

## 0.48.0 (2026-11-11)

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
