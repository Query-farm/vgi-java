# vgi-java

Java port of the VGI protocol (DuckDB extension that lets external workers
serve catalog data over Arrow IPC). Driven by passing the integration suite
at `~/Development/vgi/test/sql/integration/`. Currently **112/129 passing**.

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

- **Java 21 + `-parameters` is mandatory.** Wire field names equal Java
  parameter names. Every `VgiService` method param must be `snake_case`
  matching the corresponding Go wire struct's field tag exactly. No
  `@JsonProperty`-style override exists.
- **Allocators:** every fixture allocates via `Allocators.root()` or a
  child. `arrow.memory.debug.allocator=true` is set in test JVM args —
  leaks fail tests early.
- **`OutputCollector.emit(root)` takes ownership** of the root (see its
  javadoc). Don't close after emit.
- **No comments** unless the *why* is non-obvious. Don't restate code.

## State of play (as of 2026-05-11)

**Passing: 114/129** (was 112/129 before commits `880a5e4` /
`bdccadc` / `5cd91f0` in `vgi-rpc-java`, which fixed three layered
dict-encoded bugs: cast-rejects-memory-format, WriteChannel-position-
desync, and missing dict-batch emission between schema and record
batch).

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

Remaining 14 failures (excluding the segfault), briefly (see `git log`
for what was tried):

- `aggregate/nest_tensor.test`, `scalar/unnest_tensor.test`,
  `table_in_out/unnest_tensor_rows.test` — nested struct+list writer
  offsets in TIO/aggregate paths.
- ~~`table_in_out/echo/{all_types,nested_type_combinations}.test`,
  `filter_pushdown/enums.test` — dict-encoded round-trip in echo TIO~~
  → all_types + enums PASS; nested_type_combinations now segfaults
  (see warning above).
- `table/{column,table_function}_statistics.test` — sparse-union encoding
  for statistics RPC.
- `table/{filter_echo_partitioned,order_preservation_modes,partitioned_sequence}.test`
  — parallelism / partitioning semantics.
- `schema_reconcile.test` — writable INSERT path.
- `table/constant_columns.test` (MAP type via `ConstantColumnsFunction`),
  `constant_columns_types.test` (HUGEINT).
- `aggregate/window.test:267` — window aggregate edge case.
- `table/join_keys_pushdown.test` — C++ side suspected.
- `attach/versioned_tables_impl.test:231` — `vgi_worker_pool` diagnostic.

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
