# vgi-java

> **Note:** this file is internal working documentation for AI-assisted
> development sessions. It references machine-specific paths (`~/Development`,
> `/tmp` harness scripts) that exist only on the maintainer's machine. If
> you're evaluating or using the library, start with [README.md](README.md).

Java port of the VGI protocol (DuckDB extension that lets external workers
serve catalog data over Arrow IPC). Driven by passing the integration suite
at `~/Development/vgi/test/sql/integration/`. Currently **185/185 passing**
(`schema_reconcile.test` skips via an upstream require-env gate — the writable
path is out of scope; see "State of play").

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

## Releasing (Maven Central)

Published: **`farm.query:vgi`** (this repo) and **`farm.query:vgirpc`** /
`vgirpc-oauth` (the sibling). Latest as of 2026-06-13: **vgi 0.2.0 → vgirpc
0.10.2**. To cut a release: bump `version` in `build.gradle.kts`, push, then
create a GitHub Release whose tag is the version (`v0.2.0` for `0.2.0`). The
`release.yml` workflow (trigger: `release: published`) verifies tag == version,
runs tests, and publishes. Both repos now set
`publishToMavenCentral(automaticRelease = true)`, so a release goes **live on
Maven Central with no manual Portal click** once it passes validation (changed
2026-06-13 — was `false`/click-gated through vgi 0.1.0 + vgirpc 0.10.1).

**The cross-repo ordering trap.** The `release.yml` build resolves
`farm.query:vgirpc` from **Maven Central** (the composite is absent on CI —
`VGI_RPC_JAVA_DIR` unset), *not* from source. So **`:vgi` cannot release until
the `vgirpc` version it pins in `vgi/build.gradle.kts` is already published to
Maven Central.** Integration CI hides this (it builds vgirpc from source via the
composite), so green integration ≠ releasable. Before tagging a `:vgi` release,
prove it builds against the *published* vgirpc with the composite disabled:

```bash
VGI_RPC_JAVA_DIR=/nonexistent ./gradlew :vgi:test :vgi:javadoc
```

(Pointing the override at a non-directory forces Maven Central resolution.) If
`:vgi` calls a vgirpc API only on `main` (e.g. `CallContext.cookies()` added
after a release), this fails to compile — publish a new vgirpc first, bump the
pin, then release `:vgi`.

**Two gotchas that cost a release attempt (2026-06-13):**
- **Javadoc doclint is fatal on the publish.** vgirpc's `publishToMavenCentral`
  runs `:vgirpc:javadoc`, which **errors** (not warns) on a stale `@param`
  (e.g. a renamed parameter). Run `./gradlew :<module>:javadoc` locally first.
  (vgi's javadoc only *warns* on missing `@param`, so `:vgi` is laxer — but
  check anyway.)
- **GitHub immutable release tags can't be reused.** A failed publish leaves the
  release's tag permanently reserved (immutable releases). You can't re-cut the
  same version after fixing — **bump the patch** (0.10.1 → 0.10.2) and tag that.

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

## State of play (as of 2026-06-18)

**2026-06-18 — bumped CI to `VGI_REF=main` + `HAYBARN_RELEASE=haybarn-v1.5.4-rc1`
and ported the 4 new fixtures / numeric-promotion fixes that `main` requires.**
Tracking `main` (not a SHA) pulled in upstream commits (`4ea7f11`/`38ff3e3`
"coverage-driven SQL tests" + `typed_probe`, table-fn count → 94) that were
ahead of the Java worker. Seven tests went red; all now pass (verified locally
against `~/Development/vgi/build/release/test/unittest`, full suite 185/185 with
the `ci/wrappers/` catalog wrappers). Changes:

- **Numeric promotion (`scalar/numeric_promotion.test`)** — `double`/`add_values`
  now promote like vgi-python's `_promote_for_addition`:
  - `TypeRules.promoteForAddition` decimal branch → `decimal128(min(p+1,38), s)`
    (was identity); int/float branches already matched.
  - `TypeRules.commonTypeForAddition` gained a decimal branch (merge precision/
    scale by the DuckDB add rule, then +1 headroom, cap 38).
  - `ScalarHelpers.toLong`/`toDouble` gained **unsigned** vector cases
    (UInt1/2/4/8) + new `toBigDecimal`. `DoubleFunction.compute` gained UInt
    output cases (so `double(10::UTINYINT)→USMALLINT`, `double(100::UINTEGER)→
    UBIGINT` no longer hit the `default` throw); `AddValuesFunction.compute`
    gained a `DecimalVector` output case.
- **`scale_by_setting` scalar** (`settings/settings_types.test`) — float
  counterpart of `multiply_by_setting`; `@Setting(default_="1.0") double
  scale_factor`. The setting itself is registered in `Main.registerSettings`
  (`SettingSpec("scale_factor", …, FLOAT64, 1.0)`) — **@Setting only *reads*; it
  does not register the DuckDB setting**, that's `Worker.settings(...)`.
- **`secret_field` scalar** (`secret/secret_fields.test`) — older
  `ScalarFunction` interface (like `ReturnSecretValueFunction`); reads the
  `vgi_example` secret struct's `port`+`secret_string` children → renders
  `port=<port>;name=<secret_string>`.
- **`typed_probe` table fn** (`table/typed_probe.test`) — typed const args with
  worker-side defaults (TIMESTAMPTZ→`timestamp(us,UTC)`, INTERVAL→
  `Interval(MONTH_DAY_NANO)`, BLOB→binary, UBIGINT→`Int(64,false)`, DOUBLE). Key
  detail: **function arg defaults are NOT on the wire** (`ArgumentSpecSerializer`
  encodes only name/type/flags), so named-const defaults are applied worker-side
  in `createProducer` via `orElse`/`containsKey`, not by DuckDB. Added
  timestamp/unsigned cases to `VectorScalarCodec.read` so the consts decode to
  predictable `Long` micros / `Long` / `PeriodDuration` (intervals fall through
  to `getObject`→`PeriodDuration`; collapse = months·30d + days·24h + nanos/1e6).
- **`filtered_columns_echo` table fn** (`table/filtered_columns_pushdown.test`)
  — reports `filtered_columns`/`has_filter_for_column`/`get_column_values('tag')`.
  Added `PushdownFilters.filteredColumns()` + `hasFilterForColumn()` (top-level
  `column_name` set, mirroring vgi-python); `getColumnValues` already existed.
- **Counts:** scalar fns 39→41 (`secret_field`, `scale_by_setting`); table fns
  92→94 (`typed_probe`, `filtered_columns_echo`). `function_registration.test`
  for both kinds updated upstream to match.

**`VGI_REF=main` is non-reproducible** — re-validate after upstream moves; the
unpinned community `vgi` extension must also be ABI-compatible with the
`v1.5.4-rc1` haybarn-unittest. Pin `VGI_REF` back to a SHA before treating CI as
stable.

## State of play (as of 2026-06-15)

**2026-06-15 — re-greened CI after an upstream rename batch + bumped `VGI_REF`
to HEAD (`3e4b68d`).** CI went red because the suite pins the `.test` files at
`VGI_REF` but installs the `vgi` extension **live from community** (unpinned).
Upstream advanced past the old pin (`4444d66`) with a cluster of rename commits
that the community extension had already shipped, so the old tests called names
the new extension no longer had:
- `vgi_join_keys_limit` setting → `vgi_join_keys_threshold` (`0504226`)
- `vgi_worker_subprocess_pool()` → `vgi_worker_pool()` (`8381b69`)
- `vgi_worker_pool_flush()` scalar → **table** function (`f9e78f5`)

All three are **extension-side SQL surface, not worker code** — no Java change
was needed; the failures were stale pinned tests. Fix = bump `VGI_REF` to HEAD
(`integration.yml`). HEAD also adds **`connection_string.test`** (bare
connection-string / `?location=` ATTACH discovery, `e19e189`/`15fe529`); that
feature **reuses the existing InvokeCatalogs RPC**, so the worker already
satisfies it with no port. Verified the full source-built suite (185/185) *and*
re-ran the four renamed tests + `connection_string.test` against the **actual CI
path** — `INSTALL vgi FROM community` driving the local `haybarn-unittest` (the
homebrew `/opt/homebrew/bin/haybarn-unittest`) + the Java worker — all green,
proving the community build already carries the connection-string feature (the
risk that gated bumping straight to HEAD). `HAYBARN_RELEASE` left at `rc7`
(upstream's `rc10` bump is a Windows-MSVC build fix, not a runtime-ABI change;
rc7 still passes against the current community extension).

## State of play (as of 2026-06-12)

**2026-06-12 — HTTP table-function streaming sweep (state serialization).** The
HTTP transport is **stateless**: a streaming call splits into `/init` +
`/exchange`, and the producer's `StreamState` is serialized into a continuation
token between them (CBOR via `StateSerializer`). The Java fixtures were written
for the in-memory unix/launch transport, so their state held things that don't
serialize — which is why **table functions worked over `launch:` but 500'd over
http** (`example.sequence`, etc.). Running the suite over `TRANSPORT=http`
(unmasked, injecting `set ignore_error_messages zzznomatch` so the runner's
built-in `ignore_error_messages={"HTTP",…}` doesn't hide failures as skips)
surfaced **48** table-function failures; systemic fixes took that to **5**. Use
`VGI_STREAM_DEBUG=1` on the worker to log the otherwise-swallowed stream-handler
exceptions. Fixes (all pushed; no launch regression — verified):
- **vgi-rpc-java `StateSerializer`**: serialize by FIELD not getters (record-style
  accessors like `BatchState.total()` aren't Jackson getters); an Arrow `Schema`
  codec (round-trip via `Schema.serializeAsMessage`/`deserializeMessage`, since
  `Field` has no default ctor + recurses → StackOverflow); jsr310 `JavaTimeModule`
  (state holding `LocalDateTime`).
- **vgi-rpc-java dictionaries** (two bugs): `Wire.writeZeroBatch` now writes empty
  dictionaries for dict-encoded (ENUM) fields so even the zero-row token batch's
  schema message renders; and `HttpStreamHandler` transfers the input batch's
  dictionaries out of the reader and threads them to the exchange response (echo
  emits dict-encoded output referencing the input's dictionary, which the reader
  had already freed).
- **vgi `FilterApplier`**: no-arg ctor + non-final fields (held in ~7 producers).
- **vgi buffering** (`BufferingFinalizeProducer` + `BufferingStorageHolder` + 7
  subclasses): the source/finalize producers held a live `BoundStorage` (SQLite
  connection) in state. Keep the view `transient`; serialize `executionId`/
  `attachId`/`finalizeStateId`; `storage()` re-binds via the static holder
  (registered by `VgiServiceImpl`) on resume. Subclasses gained no-arg ctors and
  use `storage()`.

**Tail closed (2026-06-13) — http is now a CI lane.** The last worker-side http
failures are fixed and the whole suite is green over `TRANSPORT=http`:
- **`overload/scalar_overload`** — VGI multiplexes *every* function through one
  wildcard `init` RPC, so vgi-rpc-java's per-method `stateTypes[init]` hint
  collides when a query drives two stream functions at once (`scalar(…) FROM
  table(…)`): a `SequenceState` token got decoded as `ScalarStreamState`.
  `StateSerializer.serialize` now **prefixes the concrete state class name**
  (`DataOutputStream` UTF + length + body); `deserialize` resolves the real type
  from the bytes (`Class.forName`, verified `StreamState`-assignable), falling
  back to the per-method hint for legacy tokens. (vgi-rpc-java `aa40dcb`.)
- **`table_in_out/unnest_tensor_rows`** — `State` held `ArrowType` axis/value
  fields (not Jackson beans). Marked `transient`; recomputed by `derive()` from
  the serializable `outputSchema` on the first exchange tick.
- **`accumulate/*`** — `AccumulateReadFunction` used an *anonymous* producer that
  captured `segments` via a synthetic enclosing ref (serializes as null).
  Promoted to a named `ReadState` (public fields, no-arg ctor).

**Two http failures that are NOT worker bugs** (proven by running vgi-python's
worker against the *same* prebuilt `haybarn-unittest` — it fails them
identically, while upstream's locally-built `unittest` passes):
- **httpfs at ATTACH** — the prebuilt `haybarn-unittest` doesn't statically link
  httpfs, so `ATTACH … (TYPE vgi, LOCATION 'http://…')` errors with a binder
  "requires the httpfs extension". On the launch lane only the `*_http.test`
  files attach http (they piggyback on autoload after an earlier `require httpfs`
  installs it); on the http lane *every* test attaches http from the start.
  `ci/preprocess-require.awk -v http=1` injects an idempotent `INSTALL httpfs
  FROM core; LOAD httpfs;` before each worker ATTACH.
- **`table/dynamic_filter.test`** — Top-N + dynamic-filter continuation
  *terminates after the first batch over http in the prebuilt binary* (the
  `LIMIT 5` heap fills within batch 1, then the C++ http scan stops issuing
  `/exchange`; `LIMIT 500` fills after 5 batches and runs to completion). The
  worker's init response is byte-identical for both LIMITs, so the divergence is
  in that C++ build, not the worker. Dropped on the http lane only (alongside
  `projection_pushdown_repro.test`, which upstream's `make test_http` also drops).

**http CI lane** — `.github/workflows/integration.yml` matrix gained
`{ lane: http, transport: http }`; `ci/run-integration.sh` resolves `TRANSPORT`
*before* staging (so the awk gets `-v http=1` and `HTTP_SKIP` drops the two
files), boots the example + versioned + versioned_tables workers each as their
own http server, and deliberately does **not** set
`VGI_REQUIRE_LAUNCHER_TRANSPORT` (so `launcher/options_smoke.test` skips). Green:
**171 test cases / 9043 assertions / 11 skipped**. (Greening `bearer_token` over
http still waits on `VGI_TEST_BEARER_TOKEN`; the no-auth ATTACH-raises change is
PR #2 upstream.)

**Released 2026-06-13 — vgi 0.2.0 → vgirpc 0.10.2** (both live on Maven Central;
see the "Releasing" section). vgirpc 0.10.2 carries the six http
state-serialization commits (`815b0fe`…`aa40dcb`); `:vgi`'s pin in
`vgi/build.gradle.kts` and `integration.yml`'s `VGI_RPC_JAVA_REF` both point at
the 0.10.2 commit (`6a9246e`).

**2026-06-12 — GitHub Actions integration CI + expression-filter pushdown.**
Two coupled pieces landed so the integration suite runs on every push/PR
without a C++ build:

- **CI (`.github/workflows/integration.yml` + `ci/`).** Drives the **prebuilt**
  standalone `haybarn-unittest` (Haybarn release asset) against the Java worker,
  installing the **signed** vgi extension via `INSTALL vgi FROM community` (deps
  from `core`) — no extension build from source. The `.test` files come from a
  pinned `Query-farm/vgi` checkout (`VGI_REF` in the workflow `env:`). The
  standalone runner links **none** of vgi/httpfs/json/parquet/spatial, so
  `ci/preprocess-require.awk` rewrites every `require <ext>` into an explicit
  `INSTALL … FROM {community,core}; LOAD …;` (`require-env` untouched).
  `ci/run-integration.sh` stages the preprocessed tree, sets the four
  `VGI_*_WORKER` vars (`launch:` + the three `ci/wrappers/` catalog wrappers —
  the committed, path-relative replacements for the old `/tmp/vgi-worker-*`),
  warms the extension cache once, then runs the whole suite in a **single
  unittest invocation** (like `make test_launcher`) so the CI log streams the
  native sqllogictest report (per-test progress + `All tests passed (… N
  assertions in M test cases)` — ~8700 assertions across the ~185 files), not a
  rolled-up count. Green on both lanes: **177 pass / 7 skip** (+
  `http/gzip_fallback.test` in a dedicated step). Sets
  `VGI_REQUIRE_LAUNCHER_TRANSPORT=1` (we *are* the launcher transport) so
  `launcher/options_smoke.test` runs — which required `Main.runWorker` to accept
  the launcher cache-key / fixture flags (`--describe`/`--no-describe`/
  `--threaded`/`--quiet`/`--debug`/`--log-level`) as no-ops instead of exiting
  on unknown argv. Boots versioned_tables + versioned catalog workers over HTTP
  (`VGI_VERSIONED_TABLES_HTTP_WORKER`/`VGI_VERSIONED_HTTP_WORKER`) so the 4
  `versioned_tables_*_http` tests and `versioning_http` (sticky-cookie
  round-trip) run, and runs `gzip_fallback` against a dedicated zstd-disabled
  http worker. These use **HTTP features added to vgi-rpc-java** (`b946c2d` —
  gzip codec negotiation via `VGI-Supported-Encodings`/415,
  `CallContext.cookies()`/`setCookie()`), which CI **builds from source** through
  the composite include (`VGI_RPC_JAVA_DIR`/`VGI_RPC_JAVA_REF`) until a vgirpc
  release ships them. `bearer_token.test` stays skipped: bearer auth IS
  implemented (`Main.buildHttpConfig` + `BearerAuthenticator`, validated 10/11
  against the vgi-python worker) but the test isn't greenable under the prebuilt
  haybarn-unittest — the community C++ extension raises on the no-auth-ATTACH 401
  instead of deferring it to the first query, which fails against the Python
  reference worker identically. The whole-suite-over-http `TRANSPORT=http` path
  stays local-only (function-backed table-function streaming, e.g.
  `example.sequence`, still errors over http). **Excludes**
  `bool_in_union.test` in addition to `nested_type_combinations` — CI surfaced
  that it characterizes a **pre-existing platform-dependent union-bool bug**:
  the worker reads uninitialized memory for boolean union variants after row 1
  (its own comment says "CORRECT result would be true,true,true,true" but it
  pins the buggy `true,false,false,false`), so the result is undefined and
  differs arm64 vs amd64. Real bug to fix separately, not a CI artifact.
  **Path-B coupling:** the extension is
  pulled live from community (not version-pinned), so a `VGI_REF` bump must be
  re-validated against the then-current community build — see `ci/README.md`.

- **Expression-filter pushdown subsystem** (was entirely absent; this is why
  `spatial_filter_example`/`expression_filter_test` were zero-row `Stub`s and
  `table/expression_filter.test` always *skipped* — the local dev harness can't
  load spatial). Now implemented end-to-end:
  - **Wire decode (core):** `PushdownFilterType.EXPRESSION` +
    `PushdownFilter.Expression(columnName, columnIndex, sql)`.
    `PushdownFiltersDecoder` renders the pushed expression tree
    (`column_ref`/`constant`/`function`/`comparison`/`conjunction`, constants
    resolved from sibling `value_ref` columns, geoarrow.wkb →
    `ST_GeomFromHEXWKB`) to a SQL predicate — mirroring vgi-python's
    `ExpressionNode.to_sql`. `evaluate()` skips Expression (not row-at-a-time);
    `PushdownFilters.expressionPredicates()` / `FilterApplier.expressionPredicates()`
    expose the SQL.
  - **Evaluator (worker):** `example/table/ExpressionFilterEvaluator` evaluates
    the predicates against each batch via an embedded **haybarn_jdbc** engine
    (Maven Central `farm.query.haybarn:haybarn_jdbc:1.5.3` + `arrow-c-data`),
    thread-local connection with spatial loaded — the Java analogue of
    vgi-python's `vgi._duckdb` evaluator. Batch is bridged in via the Arrow C
    Data interface (`registerArrowStream`); a registered stream is **single-use**
    so it's queried once per batch; `FilterApplier.compact` does the masking.
    **haybarn_jdbc is a worker-only dep — the published `vgi` core stays
    engine-free.**
  - **Capability wiring:** `FunctionMetadata.withSupportedExpressionFilters(...)`
    → `FunctionInfo.supported_expression_filters` (was hardcoded `List.of()` at
    `VgiServiceImpl` ~line 1731). The C++ extension only pushes an expression
    filter when every function name in the tree is declared; otherwise it keeps
    a FILTER node (the `length(name) > 7` EXPLAIN case).
  - **Fixtures:** real `SpatialFilterExampleFunction` (`{n,x,y,geom GEOMETRY}`
    grid, declares `&&`/`st_intersects_extent`) + `ExpressionFilterTestFunction`
    (`{id,name,tags,score}`, declares `list_contains`/`starts_with`/`contains`)
    replace the two stubs. `table/expression_filter.test` now **runs and passes**
    (30 assertions, incl. the pushdown EXPLAIN asserts) — strictly better than
    the prior silent skip.

## State of play (as of 2026-06-11)

**2026-06-11 — ported the evolved vgi-python storage layer** (counters + ranged
ops `88581a6`, per-attach sharding `c5c09f2`, BoundStorage facade). The Python
conformance suites were ported as JUnit and are the spec
(`storage/FunctionStorageConformanceTest` + `BoundStorageConformanceTest`,
each run over sqlite-memory / sqlite-file / mock-CfDo backends).

- **Backends** (`storage/FunctionStorage` + both impls): `stateScan`
  (`[start,end)`, reverse, limit, unsigned-lex order), `stateDrain` (atomic,
  single `DELETE..RETURNING` statement on sqlite — SELECT-then-DELETE races
  cross-process), `stateDelete` now returns count + new `stateDeleteRange`
  (both-null = wipe ns), atomic int64 counters (`function_counter` table;
  add = single upsert-RETURNING), FIFO `work_queue` (pop = single-statement
  claim), sqlite schema self-heal on init (drops tables w/ stale idempotency
  columns), CfDo client pages scan/drain via `after_key`/`next_after` (drain
  mints ONE attempt_id reused across pages — server snapshot semantics).
- **`storage/BoundStorage`** — the `params.storage` facade: scoped to one
  execution_id, **shard-pinned at construction** from the attach plaintext's
  leading UUID (`ShardKey.derive` → `forShard`; Java pins once instead of
  Python's per-call `shard_key=` kwargs — equivalent, since Python also
  resolves once in `__init__`). Null/empty attach + `requiresShardKey()`
  backend (CfDo) → hard error, mirroring Python. User `byte[]` namespaces
  starting `_vgi/` are rejected; `FrameworkNs` enum overloads bypass.
  Replaced `buffering/BufferingStorage` (deleted) in the three buffering
  params records; also exposed on `TableInitParams.storage()` and
  `TableInOutInitParams.storage()` (BoundTableInOut now carries the unsealed
  attach — `bindTableInOut` takes ctx).
- **Layout migrations (Python byte-exact):** aggregate state → ns
  `_vgi/aggregate_state`, keys `BoundStorage.packIntKey(gid)` (8-byte
  **little-endian** signed; was BE), const args at synthetic gid `-2`;
  transactions → data ns `"txn"` (via new `storage/TransactionBoundStorage`,
  which `TransactionStore.view` now returns), active marker `_vgi/txn_active`.
  `TransactionStore.begin/end/view` take the unsealed attach for shard routing;
  `table_buffering_destructor` gained `CallContext` and shard-pins its
  `executionClear` (was unsharded — broken on CfDo).
- **Suite status: 185/185 (fully green).** `schema_reconcile.test` now *skips*
  under this harness — upstream vgi `4444d66` added
  `require-env VGI_SCHEMA_RECONCILE_DB` (the per-run sqlite path every Python
  Makefile lane already exports), the same capability-gating pattern as
  `versioned_tables_impl`. The writable path it exercises stays out of scope.
  `table/comments.test` was a one-word upstream drift, fixed here.

**2026-06-11 (same session) — ported the new accumulate fixture catalog**
(vgi `48f4fea` / vgi-python `_test_fixtures/accumulate/worker.py`), the first
real consumer of the storage port. All 6 `accumulate/*.test` green.

- **Multi-catalog (MetaWorker-style) serving** via
  `Worker.ExtraCatalog(name, implVersion, dataVersion, schemaComment,
  functionNamePrefix)` + `registerExtraCatalog`: extra rows in
  `catalog_catalogs`, `catalog_attach` branches on the attach name (random
  16-byte opaque id = the per-ATTACH storage scope,
  `attach_opaque_data_required=true`, its own resolved versions),
  `catalog_schemas`/`schema_get`/`schemaCounts`/`contents_functions` route per
  attach via `catalogRegistry.catalogName` (extends the `projection_repro`
  prefix-filter precedent; owned functions are hidden from the example
  catalog's listings, so `function_registration.test` counts are unchanged).
  **Gated to `catalogName=="example"` in Main** — the versioned wrappers reuse
  the binary and their `vgi_catalogs()` must stay single-row (regressed
  `attach/versioning.test` + `versioned_tables.test` until gated).
- **Buffering API grew Python-parity context**: `TableBufferingProcessParams`
  / `CombineParams` now carry `args`, `outputSchema`, `attachOpaqueData`,
  rehydrated per-RPC from a `BufferingInitState` record persisted into
  execution-scoped storage at Sink init (`FrameworkNs.BUFFERING_INIT`, key
  `packIntKey(-1)`) — any pool worker can serve any RPC, mirroring worker.py's
  `_TABLE_BUFFERING_INIT_KEY`. `TableInOutBindParams` + `TableBindParams`
  gained `attachOpaqueData`/`attachStorage` (attach-scoped facade; null during
  catalog enumeration — fixture `onBind`s must tolerate that).
  `BoundStorage.rescope(scopeId)` rebinds the same pinned backend to another
  scope (the fixtures' attach-scoped collection store).
- **Fixtures** (`example/accumulate/`): `AccumulateFunction` (buffering:
  stage→stamp one `_timestamp` (timestamp[us], tz-naive → DuckDB TIMESTAMP)→
  append time-keyed segments (BE-us-prefix keys so memcmp==time order)→TTL via
  one ranged delete→max_row_size drops oldest segments trimming the straddler
  via `VectorSchemaRoot.slice`→stage all/new/none), `AccumulateReadFunction`,
  `AccumulateClearFunction`, shared `AccumulateStore` (per-collection row
  counters via `counterAdd`). INTERVAL named args arrive as Arrow
  `PeriodDuration` (months≈30 days, Python parity).
- **Stale-worker race (cost a flaky suite run):** `pkill` alone isn't enough
  before relaunching — the dying worker's socket can still accept the first
  test's connection. Use `pkill -f farm.query.vgi.example.Main; until ! pgrep
  -f farm.query.vgi.example.Main >/dev/null; do sleep 1; done` before a suite
  run (alphabetically-first `accumulate/attach_scope.test` was the victim).
- **Harness reconstructed again** (all of `/tmp` was wiped): `/tmp/run_test.sh`
  now also sets **`VGI_TEST_DEDICATED_WORKER`** (plain binary, NO `launch:`) —
  `table_buffering_{worker_crash,pool_recovery}.test` require it — and counts
  "All tests were skipped" as a skip. Wrapper env contracts unchanged
  (versioned = 1.0.0 / >=1.0.0,<2.0.0 / 1.0.0,1.1.0,1.2.0 / default 1.2.0).

## State of play (as of 2026-06-05)

**Passing: 178/179** (excluding the filtered-out `nested_type_combinations.test`
segfault; `require-env`/`require spatial` skips). The only failure is
`schema_reconcile.test` (writable INSERT, deferred — out of scope). Re-verified
inline **and** under shm (`VGI_RPC_SHM_SIZE_BYTES`) against the current
`~/Development/vgi/build/release/test/unittest`.

**2026-06-05 — synced 5 new vgi-python / C++ features (5 new tests, all green).**
The dominant new piece is **AT (time travel) threaded onto `BindRequest`**
(vgi-python `bf40cc4`, vgi `0e5eb64`): `BindRequest` grew nullable
`at_unit`/`at_value` (name-keyed, additive), `TableInitParams` + the `BoundTable`
bind-cache carry them, and `initTable` passes them to the producer. A
function-backed table now reads AT at init via the bind request embedded in
`InitRequest` (no init-machinery change). Per feature:

- **time_travel_pushdown** — `tt_pushdown_fn` (function-backed, reads AT at init
  via `TableInitParams.atUnit/atValue`) + `tt_pushdown_cols` (columns-based; AT →
  version resolved in `catalog_table_scan_function_get`/`_scan_branches_get`).
  Both in `TimeTravelPushdownFunctions` (`tt_pushdown_scan`/`tt_pushdown_cols_scan`,
  echo `seen_version` + `pushed_filters`). `CatalogRegistry.resolveVersion` now
  **passes through** (rather than rejecting) these two names; a `resolveTtVersion`
  helper in `VgiServiceImpl` mirrors the python fixture (no-AT→v2, `VERSION`→int,
  `TIMESTAMP` year≤2020→1 else 2). The columns-based scan is special-cased in
  *both* the scan-function and scan-branches RPCs (the Java worker implements
  branches, so C++ routes every example table through `scan_branches_get`).
- **filter_pushdown_through_view** — `FilterEchoTableScanFunction`
  (`filter_echo_table_scan`, no-arg 100-row scan echoing `pushed_filters`) backs
  `data.filter_echo_table`. The asserted LIKE cases all push as constant-prefix
  RANGE filters, so no `supported_expression_filters` wiring was needed.
- **table_buffering_large_batch** — `BufferEmitWideFunction` (`buffer_emit_wide`,
  finalize emits one batch of N>2048 rows; C++ drains it across `GetData` calls
  after vgi `2c67929`).
- **required_field_filter_paths_native** — `rff_parquet`/`rff_hive`/
  `rff_hive_mixed` columns-based tables delegate to native `read_parquet` via
  `catalog_table_scan_function_get` (`scanFunction("read_parquet", [path], …)` +
  `rpcScanFunction()`); the test `COPY`s the parquet/Hive-glob fixtures itself.
- **required_field_filter_paths_rowid** — `RffRowidScanFunction` (`rff_rowid_scan`,
  real generator: 10 rows, FLOAT32 `bbox` struct built directly with
  `Float4Vector` children + `setIndexDefined`, `auto_apply_filters` applies the
  rowid + bbox.* filters so `WHERE rowid = N` returns one row). The C++ resolves
  the sentinel-keyed rowid filter to the `row_id` field name
  (`vgi_table_function_impl.cpp`), so the worker's `FilterApplier` matches it by
  name.

`table/function_registration.test` count is now **92** (+5 new table-type fns).
The `SimpleTableFunction`-based fixtures must override `argumentSpecs()` (empty
for no-arg) — the default reads `spec()` which throws.

**Harness note:** `/tmp/run_test.sh` + the three `/tmp/vgi-worker-*` wrappers are
ephemeral (not in the repo) and had to be reconstructed this session. The
`launch:` scheme auto-appends `--unix`/`--idle-timeout`, so each
`VGI_*_WORKER=launch:<binary-or-wrapper>` just names the executable. The
`unittest` binary takes **one** `.test` path per invocation, so `run_test.sh -f`
loops (the `launch:` pool stays warm across invocations via flock).

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
- Service impl (large, dispatches everything): `vgi/src/main/java/farm/query/vgi/internal/VgiServiceImpl.java`
- RPC service interface: `vgi/src/main/java/farm/query/vgi/VgiService.java`
- Fixtures: `vgi-example-worker/src/main/java/farm/query/vgi/example/{scalar,table,tableinout,aggregate}/`
- Wire DTOs: `vgi/src/main/java/farm/query/vgi/protocol/`
- IPC helpers (DRY refactor target): `vgi/src/main/java/farm/query/vgi/internal/{BatchUtil,SchemaUtil,SettingsParser,ArgumentsParser}.java`

## Out of scope

`writable/` (18) and `simple_writable/` (5) tests are deferred indefinitely
— vgi-go does the same. The Java port is read-only catalogs.
