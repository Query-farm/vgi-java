# vgi-java

Java port of the VGI protocol (DuckDB extension that lets external workers
serve catalog data over Arrow IPC). Driven by passing the integration suite
at `~/Development/vgi/test/sql/integration/`. Currently **173/174 passing**
(the 1 failure — `schema_reconcile.test` — is out of scope; see "State of play").

## Canonical references

When porting a fixture or chasing wire behaviour, look here first — these
ship the same suite and are authoritative:

- **Python (canonical):** `~/Development/vgi-python/vgi/` —
  `_test_fixtures/` is the reference for every fixture.
- **Go (closest structural parallel):** `~/Development/vgi-go/vgi/` —
  use this when picking the Java approach; struct-based dispatch translates
  more cleanly than Python's annotations.
- **RPC layer:** `~/Development/vgi-rpc-java/` (included as composite build).
  Don't reinvent `RpcServer`, `OutputCollector`, `Marshalling`, transports.
- **Wire docs:** `~/Development/vgi/docs/launcher-protocol.md`.

## How tests run

The Makefile's `test` target is **stale** (excludes `attach/` and
`bearer_auth/`, doesn't know about the wrapper scripts). Use the real
driver:

```bash
./gradlew :vgi-example-worker:installDist
find ~/Development/vgi/test/sql/integration -name '*.test' \
  -not -path '*/writable/*' -not -path '*/simple_writable/*' \
  | sort > /tmp/intest.txt
/tmp/run_test.sh -f /tmp/intest.txt        # full suite (~129 tests)
/tmp/run_test.sh path/to/single.test       # one file
```

`/tmp/run_test.sh` sets the four `VGI_*_WORKER` env vars the C++ runner
expects, then execs `~/Development/vgi/build/release/test/unittest`. It is
**not** in the repo — it lives in `/tmp/`. Same for the three worker
wrappers it points at (`/tmp/vgi-worker-versioned`,
`/tmp/vgi-worker-versioned-tables`, `/tmp/vgi-worker-attach-options`).

These wrappers route a *single* worker binary into different catalogs by
setting env vars before `exec`:

| Wrapper | `VGI_WORKER_CATALOG_NAME` | Extra env |
|---|---|---|
| (none — direct binary) | `example` (default) | — |
| `vgi-worker-versioned` | `versioned` | `IMPLEMENTATION_VERSION`, `DATA_VERSION_SPEC`, `SUPPORTED_VERSIONS`, etc. |
| `vgi-worker-versioned-tables` | `versioned_tables` | same set, different values |
| `vgi-worker-attach-options` | `attach_options` | none |

Inside `Main.java`, `catalogName.equals("attach_options")` short-circuits to
a minimal worker that registers only `echo_attach_options`. Other catalog
names get the full example fixture set. The wrappers must stay in sync if
their env contract changes — they're not generated.

> **Wrapper drift (fixed 2026-05-20):** the canonical `versioned_tables`
> fixture moved its version sets, so `/tmp/vgi-worker-versioned-tables` had
> to be updated to match `vgi-python/_test_fixtures/versioned_tables.py`:
> `SUPPORTED_VERSIONS=1.0.0,1.1.0,2.0.0,3.0.0`, `DATA_VERSION_SPEC=>=1.0.0,<4.0.0`,
> `DEFAULT_DATA_VERSION=3.0.0`, `IMPLEMENTATION_VERSION=11.0.0` (default impl),
> `SUPPORTED_IMPL_VERSIONS=10.0.0,10.1.0,11.0.0`. The `/tmp/vgi-worker-versioned`
> wrapper still matches the unchanged `versioned` fixture and was left as-is.

> **Real test driver (fixed 2026-05-21):** `/tmp/run_test.sh` now `cd`s to
> `~/Development/vgi` and execs `~/Development/vgi/build/release/test/unittest`,
> so `/tmp/run_test.sh -f /tmp/intest.txt` works directly. The canonical binary
> is **always** `~/Development/vgi/build/release/test/unittest` — *never*
> `~/Development/vgi/duckdb/build/...` (that's a wrong path; `run_test.sh` used
> to point there). The binary registers tests by path relative to
> `~/Development/vgi` but accepts both relative and absolute `.test` paths, so
> the absolute paths in `/tmp/intest.txt` are fine. After rebuilding the worker,
> `pkill -f farm.query.vgi.example.Main` before re-running.

## Shared-memory transport (implemented 2026-05-25)

The POSIX shm side-channel is wired in (all of it in **vgi-rpc-java**; the
worker needs no code changes). The C++ client creates/owns a `shm_open`
segment when `VGI_RPC_SHM_SIZE_BYTES` is set and advertises
`vgi_rpc.shm_segment_name`/`_size` on each `init` request; the Java worker
attaches and offloads streaming output batches as zero-row pointer batches.

- `shm/ShmSegment.java` — POSIX `shm_open`/`ftruncate`/`mmap`/`munmap`/
  `shm_unlink` via FFM (`java.lang.foreign`, GA in JDK 22+; needs the JDK 25
  toolchain + `--enable-native-access`). Header layout + first-fit allocator
  are byte-compatible with C++/Go/Python. `attach()` is the only production
  path (`O_RDWR`, no `O_CREAT`); the worker never unlinks (client owns it).
- `shm/ShmResolver.java` — `maybeWriteToShm` (emit) + `resolve`/`isPointer`,
  mirroring the `external/{Externalizer,LocationResolver}` pointer-batch pattern.
  Emit/resolve read/write the segment via **`MemorySegment`-backed
  `WritableByteChannel`/`ReadableByteChannel`** (`ShmSegment.writeChannelAt`/
  `readChannelAt`, 2026-05-25) handed straight to Arrow's `WriteChannel`/
  `ReadChannel` (new channel-accepting `IpcStreamWriter`/`IpcStreamReader`
  ctors). Emit estimates an upper-bound size, allocates, serializes in place,
  records the exact `written()` length (overflow → inline). **Do NOT route these
  through `Channels.newChannel(InputStream/OutputStream)`** — a JFR profile
  showed that adapter burning ~72% of worker CPU bouncing each buffer through a
  heap `byte[]`; the channel form makes each transfer one bulk `MemorySegment.
  copy`, leaving the worker genuinely copy-bound. After this, Java shm `echo` is
  the **fastest** of Java/pyarrow/Go at small–mid batches (4.2 GiB/s at 1 MiB)
  and competitive at 512 MiB. The remaining non-copy cost is Arrow-Java zeroing a
  fresh Netty-unpooled direct buffer per batch (`Bits.setMemory`, ~33%) — would
  need a pooled allocator; the pipe path (no channel) is untouched and still the
  slow fallback.
- `shm/ShmSession.java` — per-connection attach state; lazy, best-effort
  (attach failure → inline). Closed in `RpcServer.serve`'s try-with-resources.
- Both the streaming path (`flushCollector`) **and** unary results
  (`writeResult`) emit shm. The C++ client resolves shm pointer batches in two
  places: the `ReadDataBatch` scan loop (table/scalar/TIO output) and
  `ResolveUnaryShm` on the `FunctionConnection` unary path
  (`table_buffering_{process,combine,destructor}`) — added 2026-05-25 in
  `~/Development/vgi/src/vgi_function_connection.cpp` (rebuild:
  `ninja -C build/release unittest`). Catalog/bind connections never advertise
  a segment (`VgiShmSegment::Create` is only in `PerformInit`), so their
  responses stay inline regardless. Dict-encoded (ENUM) and zero-row batches
  fall back to inline; segment-full also falls back.

  > **Earlier mistake (don't repeat):** shm'ing `writeResult` *before* the C++
  > unary resolve existed broke `table_buffering_process` → "Empty response"
  > (the 0-row pointer looked empty). The C++ side must resolve a response path
  > before the worker is allowed to shm it.

**Inbound (client → worker), added 2026-05-25 — fully bidirectional now.** The
C++ client also writes request data into the *same* segment: `VgiShmSegment::
AllocateAndWrite` (first-fit, the inverse of `FreeAllocation`) +
`MaybeWriteBatchToShm` in `vgi_function_connection.cpp`, wired into
`WriteInputBatch` (scalar/TIO input) and `RpcTableBufferingProcess` (the large
unary params batch). The worker's `ShmResolver.resolve`/`isPointer` (already
present, in `serveOne` for unary params and `processOneTick` for stream input)
light up; **client allocates, worker frees** (inverse of outbound) — race-free
under lockstep, verified under `threads=8`.

Two subtleties that cost time (don't regress):
- The client must serialize the shm bytes under the **bind-time `input_schema_`**
  (the schema `input_writer_` declares on the wire), *not* `batch->schema()` —
  Arrow's stream writer leniently writes the batch's buffers under the declared
  schema, and TIMESTAMP_TZ etc. normalize differently, so a `batch->schema()`
  serialization makes the worker's resolved schema ≠ `inputSchema` → a doomed
  cast (`ClassCastException: TIMESTAMPMICROTZ`).
- `ShmResolver.resolve` materializes via **`TransferPair`** (not row-wise
  `copyFromSafe`, which `ComplexCopier` rejects for `LIST<TIMESTAMP_TZ>`) and
  labels the result with the **pointer batch's schema** (== `inputSchema`), so
  the downstream equality check passes. Dict/ENUM inputs stay inline (the worker
  decodes resolved params with a null `DictionaryProvider`).

Run it: `VGI_RPC_SHM_SIZE_BYTES=67108864 [VGI_RPC_SHM_DEBUG=1
VGI_WORKER_STDERR=/tmp/w.log] /tmp/run_test.sh -f /tmp/intest.txt`. Verified:
full suite identical to inline (164/165, only `schema_reconcile`); a tiny
`VGI_RPC_SHM_SIZE_BYTES=69632` forces mid-stream fallback and still passes.
`VGI_RPC_SHM_DEBUG=1` logs `[vgi-shm] attached …` (worker) and
`[shm] resolved batch …`/`inline fallback …` (C++ client).

## The `launch:` prefix matters

Workers are slow to cold-start (~2–5 s JVM). The C++ extension supports a
`launch:<argv>` LOCATION scheme that starts the worker once via
flock-coordinated AF_UNIX socket, then reuses it across all queries. **Every
`VGI_*_WORKER` env var in `/tmp/run_test.sh` uses `launch:`** — remove it
and the full suite goes from ~30 s to many minutes.

Worker code path: `Worker.runUnixSocket(Path, idleMs)` →
`UnixSocketTransport.serveForever`. Prints `UNIX:<path>\n` to stdout once
bound; the launcher reads that.

To debug a launcher-mode worker crash, set `VGI_WORKER_STDERR=/tmp/w.log`
in the wrapper script (launcher dup2's `/dev/null` over fd 2).

## Composite build hazard

vgi-rpc-java is included via `settings.gradle.kts` as a composite build.
Gradle's incremental cache occasionally misses RPC-layer edits — if a
change to `~/Development/vgi-rpc-java/` doesn't show up, run
`./gradlew --refresh-dependencies` or `./gradlew clean`.

## Conventions

- **Java 25 + `-parameters` is mandatory.** (Bumped from 21 → 25 on
  2026-05-25 so the shm transport can call POSIX `shm_open`/`mmap` via the
  GA `java.lang.foreign` FFM API without `--enable-preview`. Both repos
  build on JDK 25; vgi-rpc-java's Gradle wrapper moved 8.10 → 9.0 because
  8.10 can't run on JDK 25. Worker + test JVMs pass
  `--enable-native-access=ALL-UNNAMED`.) Wire field names equal Java
  parameter names. Every `VgiService` method param must be `snake_case`
  matching the corresponding Go wire struct's field tag exactly. No
  `@JsonProperty`-style override exists.
- **Allocators:** every fixture allocates via `Allocators.root()` or a
  child. `arrow.memory.debug.allocator=true` is set in test JVM args —
  leaks fail tests early.
- **`OutputCollector.emit(root)` takes ownership** of the root (see its
  javadoc). Don't close after emit.
- **No comments** unless the *why* is non-obvious. Don't restate code.

## Scalar fixtures: use `ScalarFn`

New scalar functions extend `farm.query.vgi.scalar.ScalarFn` and declare
a single `compute()` method whose annotated parameters drive the entire
spec + dispatch:

```java
public final class MultiplyFunction extends ScalarFn {
    @Override public String name() { return "multiply"; }
    @Override public String description() { return "Multiplies a value by a factor"; }

    public void compute(@Vector BigIntVector value, @Const long factor, BigIntVector result) {
        // loop
    }
}
```

Parameter rules:
- `@Vector <ArrowVectorClass>` — per-row input column.
- `@Vector(any=true) FieldVector` — accepts any Arrow type. Combine with
  `typeBound = TypeBoundPredicate.IS_ADDABLE` for bind-time validation.
- `@Vector(varargs=true) List<X>` — varargs of typed vectors.
- `@Vector(any=true, varargs=true) List<FieldVector>` — varargs of any type.
- `@Const <java type>` — bind-time positional const arg. Type mapping:
  `long/int → INT64`, `double → FLOAT64`, `String → UTF8`, `boolean → BOOL`,
  `byte[] → BINARY`.
- `@Setting <java type>` — session setting; same type mapping. Optional
  `default_ = "..."`.
- `@OutputLength int` — injected batch row count, for functions with no
  vector input.
- Last unannotated Arrow-vector parameter = output, framework-allocated,
  `void` return.

Override hooks for the hard cases (used by `Double`, `AddValues`, the geo
fixtures, `BinaryPacket`):
- `outputType(Schema, Arguments)` — dynamic scalar output type.
- `outputSchema(Schema, Arguments)` — non-flat outputs (STRUCT, LIST,
  FixedSizeList) with explicit children.
- `argumentSpecs()` — for nested Arrow types whose children can't be
  inferred from the Java vector class.

Type-bound violations are reported at bind time with a function-named,
SQL-typed message (e.g. `add_values: col1 must be numeric (got VARCHAR)`).
`ReturnSecretValueFunction` remains on the older `ScalarFunction` interface
pending a secrets-accessor design.

The table / table-in-out / buffering / aggregate kinds **still use the
older interfaces** (`TableFunction`, `TableInOutFunction`, etc.) — the
`ScalarFn` style hasn't been extended to those because their richer
lifecycle methods + per-execution state don't translate one-for-one.

## State of play (as of 2026-06-01)

**Passing: 173/174** (excluding the filtered-out `nested_type_combinations.test`
segfault; 15 `require-env`/`require spatial` skips). The only failure is
`schema_reconcile.test` (writable INSERT, deferred — out of scope). Re-verified
against the current `~/Development/vgi/build/release/test/unittest`.

**2026-06-01 — synced the `required_field_filter_paths` feature** (vgi-python
`7f4d80a`, vgi `f7eb2d1`). `TableInfo` grew a final `required_field_filter_paths:
list<utf8>` wire field — adding it was the dominant fix (its absence broke the
`catalog_schema_contents_tables` item schema, which cascaded into ~30 unrelated
catalog-listing failures). Changes: `protocol/TableInfo` + `TableInfoSerializer`
gained the field; `CatalogTable` gained a `requiredFieldFilterPaths` record
component (threaded through the builder + every `with*` method, plus
`withRequiredFieldFilterPaths`); `VgiServiceImpl.toTableInfo` emits it. The C++
`VgiRequiredFiltersOptimizer` reads the inline paths and enforces them at bind
time. Fixtures: 5 `rff_*` catalog tables in `Main.registerCatalogTables`
(`rff_simple`/`_struct`/`_nested`/`_multi`/`_none`, backed by `_table_data` canned
data — `CannedDataFunction`'s struct writer is now recursive so `wrapper.mid.leaf`
round-trips) + 5 zero-row `rff_*_scan` stub functions in `StubFunctions` (for the
`function_registration.test` count, now **87** table functions). Greened all 6
`table/required_field_filter_paths_*.test`.

(The earlier `083b8af` commit on this branch already synced the sibling
view-column-comments + late-mat + value-prune features; `ViewInfo.column_comments`
and `FunctionInfo.late_materialization` were in place before this session.)

> **`attach/versioned_tables_impl.test` resolved upstream (2026-05-21):** the
> previously not-Java-fixable launcher-pool failure is gone — vgi gated it
> behind `require-env VGI_REQUIRE_LAUNCHER_TRANSPORT`, so it now *skips* under
> `launch:` transport instead of failing on empty `vgi_worker_pool` rows. No
> Java change was needed.

**2026-05-20 — ported a batch of new vgi-python / C++ features.** Done and
verified against the live suite:

**2026-05-20 — ported a batch of new vgi-python / C++ features.** Done and
verified against the live suite:

- **protocol_version** — `Worker.VGI_PROTOCOL_VERSION = "1.0.0"` →
  `RpcServer.setProtocolVersion` in `buildServer`. (No suite test yet;
  vgi-rpc-java only feeds it to the access log.)
- **catalog releases + source_url** — `CatalogInfo` wire schema grew to 6
  fields (`releases: list<struct<version, released_at, summary, notes_url>>`,
  `source_url: utf8?`). New `CatalogDataVersionRelease` record;
  `CatalogInfoSerializer` rewritten; `Worker.releases(...)/.sourceUrl(...)`;
  versioned_tables catalog populates the canonical manifest. Greened
  `versioning.test` + `versioned_tables.test` (were failing on the 4-field
  schema). Also fixed the stale `/tmp/vgi-worker-versioned-tables` env →
  greened `versioned_tables_resolved/_spec`.
- **projection pushdown for table-in-out** — `initTableInOut` now narrows the
  output schema by `projection_ids` when the fn opts into pushdown (mirrors
  `initTable`). New `EchoWitnessFunction` (`echo_witness`). Greened
  `pushdown_witness.test`.
- **multi-branch scan** — `ScanBranch` model + `ScanBranchesResultSerializer`
  (matches C++ `ScanBranchSchema`/`ScanBranchesResultSchema`);
  `VgiService.catalog_table_scan_branches_get` + impl. **The C++ branches
  capability is cached per-attach, not per-table**, so the impl returns a
  valid result for *every* scannable table (1-branch wrap for regular tables,
  N-branch for multi-branch). `Worker.registerMultiBranchTable(stub, branches)`
  side-registry; 7 fixtures in `Main.registerMultiBranch`. `numbers` switched
  from `make_series` → `sequence` to match canonical (VGI maps scan output to
  declared columns positionally, so column `value` is preserved). Greened all
  10 `catalog/multi_branch_*.test` + `comments.test`.
- **filter_pushdown subtypes** — all 12 `filter_pushdown/*.test` already pass;
  no work needed.
- **echo projection pushdown** — `EchoFunction` now opts into projection
  pushdown and selects the (framework-narrowed) output columns by name in
  `onInputBatch`, so no narrowing PROJECTION node sits above INOUT_FUNCTION.
  Greened `echo/projection_filters.test`.
- **table buffering (Sink+Source)** — full new execution subsystem.
  - Wire: `InitRequest` gained `finalize_state_id`; new packed DTOs
    `TableBuffering{Process,Combine,Destructor}{Request,Response}`
    (`protocol/`) matching the C++ inner schemas.
  - Service: `VgiService.table_buffering_{process,combine,destructor}`
    (process/combine take `CallContext` for `ctx.clientLog(...)`); dispatch +
    a `BoundBuffering` bind path + an `initBuffering` that handles phase
    `TABLE_BUFFERING` (Sink — mints execution_id, header-only stream) and
    `TABLE_BUFFERING_FINALIZE` (Source — one producer per finalize_state_id,
    reusing `RpcStream.producer` + projection-narrowing).
  - API (`buffering/` package): `TableBufferingFunction` (process → state_id,
    combine → finalize_state_ids, createFinalizeProducer), `BufferingStore`
    (in-process append-log keyed by `execution_id` — sufficient because the
    launcher runs a single long-lived worker), `BufferingStorage` view,
    `BufferingFinalizeProducer` (narrows full buffered batches to the projected
    schema by name + applies pushdown filters). Ordering knobs
    (`sourceOrderDependent`/`sinkOrderDependent`/`requiresInputBatchIndex`)
    ride on the interface and are re-stamped onto the wire `FunctionInfo`.
    `FunctionInfoSerializer` FUNCTION_TYPE dict gained `"table_buffering"`.
  - Fixtures (`example/buffering/`): `buffer_input`, `sum_all_columns`,
    `echo_buffering`, `ordered_buffer_input`, `batch_index_buffer_input`,
    `large_state`, `ordered_source`, `slow_cancellable_buffering`, and the four
    `crash_on_{process,combine,finalize}`/`hang_on_process` injectors.
    `buffer_input` and `sum_all_columns` moved from table-in-out to buffering to
    match canonical (the TIO `SumAllColumnsFunction` class stays for
    `DistributedSumFunction`/`ExceptionProcessFunction` to extend, just
    unregistered as `sum_all_columns`).
  - Greened all 15 `table_in_out/table_buffering_*.test` (incl. the
    crash/pool/worker_crash variants), `table_in_out/sum_all_columns.test`,
    `table_in_out/logging.test`, and `table/function_registration.test` (79
    example table functions).
  - Go has **no** buffering impl — Python (`vgi/table_buffering_function.py`,
    `_test_fixtures/table_in_out.py`) is the only reference.

---

Prior state (132/134): re-greened after vgi-python /
vgi C++ landed the `attach_id`→`attach_opaque_data` rename plus
batch_index v1, partition_columns v2, transaction storage, and AEAD.
Work that closed the gap:

- **`attach_id`/`transaction_id` → `*_opaque_data` rename** across every
  wire DTO and `VgiService` method (C++ `082104c` / Python `6a8d97c`).
- **batch_index v1** — `FunctionMetadata.withBatchIndex()` +
  `EmitMetadata.batchIndex(...)` emit the `vgi_batch_index` per-batch
  tag. Fixtures `partitioned_batch_index{,_marked}` +
  `broken_{missing_batch_index_tag,non_monotone_batch_index,batch_index_overflow}`.
- **partition_columns v2** — `FunctionMetadata.withPartitionKind(...)` +
  `EmitMetadata.partitionField(...)` / `partitionValues(...)` emit
  `vgi_partition_values#b64`. Fixtures `country_partitioned_sales`,
  `region_year_partitioned`, `partitioned_with_explicit_override`,
  `disjoint_range_partitioned`, plus four `broken_partition_*` fixtures
  (two raise worker-side in `EmitMetadata`, two reach the C++
  `InstallBatch` defense-in-depth check).
- **transaction storage** — `TransactionStore` + `TransactionStorage`
  view threaded through `TableBindParams`; `catalog_transaction_begin/
  commit/rollback` wired; `supports_transactions=true` on the attach
  result. Fixture `tx_cached_value` ships its resolved value
  bind→producer via `BindResponse.opaque_data` /
  `TableInitParams.bindOpaqueData`.
- **GEOMETRY stats** — `geo_points` catalog table (`geoarrow.wkb`-typed
  `geom` column) + `ColumnStatistics.ofGeometry(...)` (WKB corner-point
  min/max); `ColumnStatisticsSerializer` now handles `binary` union
  children. `column_statistics.test` fully green.
- **AEAD opaque-data sealing** — `OpaqueDataSealer` (ChaCha20-Poly1305,
  AAD-bound to `(domain, principal)`; transaction envelope additionally
  binds its parent attach) wired into `VgiServiceImpl`. **HTTP-transport
  only**: stdio / AF_UNIX construct it disabled, so it is pure
  passthrough for the integration suite — `Worker.buildServer(boolean)`
  passes `true` only from `runHttp`. Unit-tested in `OpaqueDataSealerTest`.

⚠️ **`table_in_out/echo/nested_type_combinations.test` SEGFAULTS the
C++ harness mid-run.** Filter it out of integration runs:

```bash
find ~/Development/vgi/test/sql/integration -name '*.test' \
  -not -path '*/writable/*' -not -path '*/simple_writable/*' \
  -not -name 'nested_type_combinations.test' \
  | sort > /tmp/intest.txt
```

Until that test is fixed, including it crashes the runner before it
finishes the suite. The segfault is downstream of the dict-batch fix
— previously this test silently returned 0 rows; now we emit data
that exercises a separate bug.

**Root cause** (traced via wire-byte capture and pyarrow diff against
Python's worker):  with `SET arrow_lossless_conversion = true`,
DuckDB sends `list<enum>` on the wire as
`list<sparse_union<varchar: dict<...>=24, uint1: uint8=33>>` —
sparse-union-tagged elements that preserve enum vs NULL identity.
Python's worker collapses this back to plain `list<dict<...>>` before
emit; Java's `EchoFunction` TransferPair-passes the sparse-union
through unchanged, and DuckDB segfaults trying to read its own
lossless encoding back through a wire shape that doesn't match the
bind-time schema. Fix lives in the fixture-side handling of
lossless-tagged inputs (probably needs to detect sparse_union
children of list/struct and re-collapse them to their declared
type before emit).

Remaining 1 failure (excluding the segfault) — out of scope:

- `schema_reconcile.test` — writable INSERT path (out of scope; `writable/`
  is deferred indefinitely).

> **Was 2 failures — `attach/versioned_tables_impl.test:231` resolved upstream
> 2026-05-21.** It used to fail because `vgi_unary_rpc.cpp:141` short-circuits
> the `vgi_worker_pool` for `launch:` transport ("the long-lived worker behind
> the socket is itself the pool"), returning 0 rows. vgi now gates the test
> behind `require-env VGI_REQUIRE_LAUNCHER_TRANSPORT`, so it *skips* under
> `launch:` rather than failing — no Java change needed. (Switching
> VGI_VERSIONED_TABLES_WORKER off `launch:` also fixed it but broke 2 other
> versioned_tables tests needing launcher data-version env propagation, so the
> upstream skip-guard is the right resolution.)

> **Launcher gotcha:** `launch:` workers are long-lived and reused across
> the whole suite via flock. After rebuilding the worker, `pkill -f
> farm.query.vgi.example.Main` before re-running or the launcher serves
> the stale binary (symptom: "function does not exist" for new fixtures,
> or duplicated rows from a half-killed pool).

## Statistics RPC

Per-column stats flow through three coordinated channels — all three
required for DuckDB's optimizer to see the stats:

1. **`CatalogAttachResult.supports_column_statistics = true`** — without
   this the catalog-level capability flag, `VgiTableEntry::GetStatistics`
   short-circuits to `nullptr` regardless of per-table data.
2. **`CatalogTable.withStatistics(List<ColumnStatistics>)`** — per-table
   inline stats encoded into `TableInfo.column_statistics` via
   `ColumnStatisticsSerializer`. C++ deserializes via
   `PopulateStatsCacheFromInline` (no separate RPC).
3. **`TableFunction.statistics(TableBindParams)`** — for function-only
   bindings (e.g. `example.sequence(N)` directly), DuckDB calls the
   `table_function_statistics` RPC. Java resolves the function via
   `OverloadResolver` and serializes `ColumnStatistics` back through the
   same sparse-union wire shape.

The sparse-union encoding for `min`/`max` in the stats batch is built
manually in `ColumnStatisticsSerializer` (Arrow Java has no high-level
sparse-union writer); per-row type codes are written directly into
`UnionVector.getTypeBuffer()` after VSR allocation.

## File bookmarks

- Worker entrypoint: `vgi-example-worker/src/main/java/farm/query/vgi/example/Main.java`
- Service impl (large, dispatches everything): `vgi-core/src/main/java/farm/query/vgi/internal/VgiServiceImpl.java`
- RPC service interface: `vgi-core/src/main/java/farm/query/vgi/VgiService.java`
- Fixtures: `vgi-example-worker/src/main/java/farm/query/vgi/example/{scalar,table,tableinout,aggregate}/`
- Wire DTOs: `vgi-core/src/main/java/farm/query/vgi/protocol/`
- IPC helpers (DRY refactor target): `vgi-core/src/main/java/farm/query/vgi/internal/{BatchUtil,SchemaUtil,SettingsParser,ArgumentsParser}.java`

## Out of scope

`writable/` (18) and `simple_writable/` (5) tests are deferred indefinitely
— vgi-go does the same. The Java port is read-only catalogs.
